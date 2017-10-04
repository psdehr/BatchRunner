using Ionic.Zip;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Text.RegularExpressions;

namespace BatchRunner
{
    class BatchRunner
    {
        private string mailFrom;
        private string emailBody;
        private string emailSubject;
        private string excludedFields;
        private string attachmentFileName;
        private bool appendDateToAttachmentFileName;
        private bool zipAttachmentFile;
        private string altAttachmentStorageFilepath;
        private string defaultOutputFormat;
        private string delimiter;
        private string zipFilePath;
        private string tempFilePath;
        private bool exceptionSentOut;
        private string sqlString;
        private SqlConnection dbConnection;
        private SqlDataReader databaseResultsReader;
        private FileStream fs;
        private bool writeOutFailed;
        private string sqlFilePath;
        private Configuration config;
        private string configFileFolder { get; set; }
        private string dbConnectionString { get; set; }
        private int sqlTimeout { get; set; }
        private string smtpHostname { get; set; }
        private string mailTo { get; set; }
        private string mailErrorsTo { get; set; }

        static void Main(string[] args)
        {
            // TODO: grab first argument as filepath of sql/config and pass to batchrunner constructor

            BatchRunner br = new BatchRunner(args[0]);

            br.InitDbConnection();
            br.GetSqlString();
            br.RunSql();
            br.PlaceAttachmentAtAltLocation();
            br.CreateOutputFile();
            br.MailOutResults();
            br.CloseDbConnection();

        }


        public BatchRunner(string configFilePath)
        {
            this.configFileFolder = Path.GetDirectoryName(configFilePath);
            this.AssignNewRuntimeConfig(configFilePath);
            this.dbConnectionString = this.GetAppSettingConfigValue("DS");
            this.sqlTimeout = Convert.ToInt32(this.GetAppSettingConfigValue("SqlTimeout"));
            this.smtpHostname = this.GetAppSettingConfigValue("SMTPHostname");
            this.mailTo = Regex.Unescape(this.GetAppSettingConfigValue("MailTo"));
            this.mailErrorsTo = Regex.Unescape(this.GetAppSettingConfigValue("MailErrorsTo"));
            this.mailFrom = this.GetAppSettingConfigValue("MailFrom");
            this.emailBody = this.GetAppSettingConfigValue("EmailBody");
            this.emailSubject = this.GetAppSettingConfigValue("EmailSubject");
            this.excludedFields = this.GetAppSettingConfigValue("ExcludedFields");
            this.attachmentFileName = this.GetAppSettingConfigValue("AttachmentFileName");
            this.appendDateToAttachmentFileName = (this.GetAppSettingConfigValue("AppendDateToAttachmentFile").ToString().Equals("False") || this.GetAppSettingConfigValue("AppendDateToAttachmentFile").ToString().Equals("0")) ? false : true;
            this.zipAttachmentFile = (this.GetAppSettingConfigValue("ZipAttachmentFile").ToString().Equals("False") || this.GetAppSettingConfigValue("ZipAttachmentFile").ToString().Equals("0")) ? false : true;
            this.altAttachmentStorageFilepath = this.GetAppSettingConfigValue("AltAttachmentStorageFilePath");
            this.defaultOutputFormat = this.GetAppSettingConfigValue("OutputFormat");
            this.delimiter = this.GetAppSettingConfigValue("Delimiter");
        }


        private string GetAppSettingConfigValue(string property)
        {
            return ConfigurationManager.AppSettings[property]; // See if this works
        }

        private void CloseDbConnection()
        {
            try
            {
                this.dbConnection.Close();
            }
            catch (Exception e)
            {
                if (!this.exceptionSentOut)
                {
                    Mailer.Send(this.mailErrorsTo, this.mailFrom, "Exception running BatchRunner job", TraceError(e), this.smtpHostname);
                }
            }
        }

        private void MailOutResults()
        {
            try
            {
                if (!this.mailTo.ToString().Equals(""))
                {
                    if (this.zipAttachmentFile)
                        Mailer.att = new Attachment(this.zipFilePath);
                    else
                        Mailer.att = new Attachment(this.tempFilePath);

                    Mailer.Send(this.mailTo, this.mailFrom, this.emailSubject, this.emailBody, this.smtpHostname);
                }
            }
            catch (Exception e)
            {
                if (!this.exceptionSentOut)
                {
                    Mailer.Send(this.mailErrorsTo, this.mailFrom, "Exception running BatchRunner job", TraceError(e), this.smtpHostname);
                }

            }
        }

        private string TraceError(Exception e)
        {
            string errorMessage = string.Empty;
            System.Diagnostics.StackTrace stackTrace = new System.Diagnostics.StackTrace();
            System.Diagnostics.StackFrame stackFrame = stackTrace.GetFrame(1);

            errorMessage += "\nException DateTime: " + DateTime.Now;
            errorMessage += "\nException at method: " + stackFrame.GetMethod().Name + ", Exception message: " + e.Message;

            System.Diagnostics.Trace.WriteLine(errorMessage);
            return errorMessage;
        }

        private void CreateOutputFile()
        {
            try
            {
                string lTempFileName = "";


                if (this.attachmentFileName != null && !this.attachmentFileName.Equals(""))
                    lTempFileName = this.attachmentFileName;

                if (this.appendDateToAttachmentFileName)
                {
                    // Get anything after and including "."
                    int extensionLength = lTempFileName.Length - lTempFileName.IndexOf(".");
                    string fileExtension = lTempFileName.Substring(lTempFileName.IndexOf("."), extensionLength);
                    string newFileName = lTempFileName.Substring(0, lTempFileName.IndexOf("."));

                    newFileName += "_" + DateTime.Now.Year + "_" + DateTime.Now.Month + "_" + DateTime.Now.Day + "_" + DateTime.Now.Hour + "_" + DateTime.Now.Minute + "_" + DateTime.Now.Second;
                    newFileName += fileExtension;
                    lTempFileName = newFileName;
                }

                // Create the temporary file name and its path
                this.tempFilePath = this.configFileFolder + "\\" + lTempFileName;

                ExcelWriter jeffExcelWriter = new ExcelWriter();

                if (this.defaultOutputFormat.Equals("Excel"))
                {
                    // Create the excel file from the dataReader resultset
                    this.fs = new FileStream(this.tempFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
                    jeffExcelWriter.CreateExcelFile(this.databaseResultsReader, this.fs, this.excludedFields);
                    fs.Close();
                }
                else
                {
                    jeffExcelWriter.CreateTextFile(this.databaseResultsReader, this.tempFilePath, this.excludedFields, this.delimiter);
                }

                if (this.zipAttachmentFile)
                {
                    CompressFile();
                }

                if (this.altAttachmentStorageFilepath != null && !this.altAttachmentStorageFilepath.Equals(""))
                    PlaceAttachmentAtAltLocation();
            }
            catch (Exception e)
            {
                // If writing out output file fails, re-attempt to write it out with a different file name.
                // Here I'm making it append date to the file name, and re-calling the same method.
                // To avoid infinite loops, I'm using writeOutFailed class variable.

                if (!this.writeOutFailed && !this.appendDateToAttachmentFileName)
                {
                    Console.WriteLine("\n\n######## Could not create output / attachment file.  Re-attempting with a different (date stamped) file name");
                    this.writeOutFailed = true;
                    this.appendDateToAttachmentFileName = true;
                    this.emailBody += "\n\nAttachment file is date stamped as the process could not write to the original file";
                    CreateOutputFile();
                }
                else
                {
                    Console.WriteLine("\n\n#### Could not create output / attachment file.  Exception as follows");
                    Console.WriteLine(e.Message + "\n" + e.StackTrace);
                    if (!this.exceptionSentOut)
                    {
                        Mailer.Send(this.mailErrorsTo, this.mailFrom, "Exception running BatchRunner job", TraceError(e), this.smtpHostname);
                        this.exceptionSentOut = true;
                    }
                }
            }
        }

        private void PlaceAttachmentAtAltLocation()
        {
            string attachmentFileName = string.Empty;
            string attachmentFilePath = string.Empty;

            if (this.zipAttachmentFile)
                attachmentFilePath = this.zipFilePath;
            else
                attachmentFilePath = this.tempFilePath;

            try
            {
                // Get attachment file name only
                int fileNameLength = attachmentFilePath.Length - attachmentFilePath.LastIndexOf("\\");
                attachmentFileName = attachmentFilePath.Substring(attachmentFilePath.LastIndexOf("\\"), fileNameLength);
                string newLocationAttachmentFilePath = this.altAttachmentStorageFilepath + attachmentFileName;
                File.Copy(attachmentFilePath, newLocationAttachmentFilePath, true);
            }
            catch (Exception e)
            {
                Console.WriteLine("\n\n\n####### Exception copying output file to alternative location.  Exception as follows");
                Console.WriteLine(e.Message + "\n" + e.StackTrace);
                if (!this.exceptionSentOut)
                {
                    Mailer.Send(this.mailErrorsTo, this.mailFrom, "Exception running BatchRunner job", TraceError(e), this.smtpHostname);
                    this.exceptionSentOut = true;
                }
            }
        }

        // Will compress the output file
        private void CompressFile()
        {
            try
            {
                this.zipFilePath = this.tempFilePath.Substring(0, this.tempFilePath.IndexOf("."));
                this.zipFilePath += ".zip";

                using (ZipFile zip = new ZipFile())
                {
                    zip.AddFile(this.tempFilePath, "");
                    zip.Save(this.zipFilePath);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("\n\n##### Could not zip a file.  Exception as follows");
                Console.WriteLine(e.Message + "\n" + e.StackTrace);
                if (!this.exceptionSentOut)
                {
                    Mailer.Send(this.mailErrorsTo, this.mailFrom, "Exception running BatchRunner job", TraceError(e), this.smtpHostname);
                    this.exceptionSentOut = true;
                }
            }

            // Try to delete the original file
            try
            {
                File.Delete(this.tempFilePath);
            }
            catch (Exception e)
            {
                Console.WriteLine("\n\n ###### Could not delete the original output file.  Exception as follows");
                Console.WriteLine(e.Message + "\n" + e.StackTrace);
                if (!this.exceptionSentOut)
                {
                    Mailer.Send(this.mailErrorsTo, this.mailFrom, "Exception running BatchRunner job", TraceError(e), this.smtpHostname);
                    this.exceptionSentOut = true;
                }
            }
        }

        private bool RunSql()
        {
            try
            {
                // Database properties inits
                SqlCommand getRecordsCommand = new SqlCommand(this.sqlString, this.dbConnection);
                getRecordsCommand.CommandTimeout = this.sqlTimeout;
                this.databaseResultsReader = getRecordsCommand.ExecuteReader();
            }
            catch (Exception e)
            {
                Console.WriteLine("\n\n\n##### Could not read database records.  Exception as follows");
                Console.WriteLine(e.Message + "\n" + e.StackTrace);

                if (!this.exceptionSentOut)
                {
                    Mailer.Send(this.mailErrorsTo, this.mailFrom, "Exception running BatchRunner job", TraceError(e), this.smtpHostname);
                    this.exceptionSentOut = true;
                }
                return false;
            }
            return true;
        }

        private void GetSqlString()
        {
            string lGetSqlStringFilePath = string.Empty;

            try
            {
                this.sqlFilePath = GetAppSettingConfigValue("SqlFilePath");

                // If full path given in config then use it
                if (File.Exists(this.sqlFilePath))
                    lGetSqlStringFilePath = this.sqlFilePath;
                else // Else try sql file path inside config folder
                {
                    string lTempFilePath = this.configFileFolder + "\\" + this.sqlFilePath;
                    if (File.Exists(lTempFilePath))
                        lGetSqlStringFilePath = lTempFilePath;
                }

                string line = string.Empty;

                using (StreamReader reader = new StreamReader(lGetSqlStringFilePath))
                {
                    while ((line = reader.ReadLine()) != null)
                    {
                        this.sqlString += "\n" + line;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("\n\n##### Could not open/read SQL script file.  Exception as follows");
                Console.WriteLine(e.Message + "\n" + e.StackTrace);
                if (!this.exceptionSentOut)
                {
                    Mailer.Send(this.mailErrorsTo, this.mailFrom, "Exception running BatchRunner job", TraceError(e), this.smtpHostname);
                    this.exceptionSentOut = true;
                }
            }
        }

        private void InitDbConnection()
        {
            if (dbConnection == null)
            {
                this.dbConnection = new SqlConnection(this.dbConnectionString);

                try
                {
                    dbConnection.Open();
                }
                catch (Exception e)
                {
                    Console.WriteLine("\n\n\n##### Could not open database using connection string: " + dbConnectionString + ".  Exception as follows");
                    Console.WriteLine(e.Message + "\n" + e.StackTrace);
                }
            }
        }

        private void AssignNewRuntimeConfig(string configFilePath)
        {
            try
            {
                Configuration config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
                ExeConfigurationFileMap configFileMap = new ExeConfigurationFileMap();
                configFileMap.ExeConfigFilename = configFilePath;
                this.config = ConfigurationManager.OpenMappedExeConfiguration(configFileMap, ConfigurationUserLevel.None);
            }
            catch (Exception e)
            {
                if (!this.exceptionSentOut)
                {
                    Mailer.Send(this.mailErrorsTo, this.mailFrom, "Exception running BatchRunner job", TraceError(e), this.smtpHostname);
                    this.exceptionSentOut = true;
                }
                throw;
            }
        }

        static partial class Mailer
        {
            public static Attachment att = null;

            public static void Send(string to, string from, string subject, string body, string smtpHostname)
            {
                try
                {
                    MailMessage message = new MailMessage();

                    message.To.Add(to);
                    message.From = new MailAddress(from);
                    message.Body = body;
                    message.Subject = subject;

                    if (att != null)
                        message.Attachments.Add(att);

                    SmtpClient client = new SmtpClient();
                    client.Host = smtpHostname;
                    object userState = message;
                    client.Send(message);
                    message.Dispose();

                }
                catch (System.Net.Mail.SmtpException se)
                {
                    Console.WriteLine("\n\n\n######## Could not send e-mail.  Exception as follows");
                    Console.WriteLine(se.Message + "\n" + se.StackTrace);
                }
                catch (Exception e)
                {
                    Console.WriteLine("\n\n\n######## Could not send e-mail.  Exception as follows");
                    Console.WriteLine(e.Message + "\n" + e.StackTrace);
                }
                finally
                {

                }
            }
        }



        
    }
}
