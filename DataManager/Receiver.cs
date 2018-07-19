using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataManager.Network.JSON;
using DataManager.Network;
using Newtonsoft.Json;
using DataManager.HouseKeeping;


namespace DataManager
{
    /// <summary>
    /// Singleton Class to create data receiver, to manage the client data
    /// </summary>
    public class Receiver
    {
        
        /// <summary>
        /// TCP Server to receive client messages
        /// </summary>
        private DataManager.Network.TCPServer tcpServer;

        private String pathConigFile;

        private int listenPort;

        private DataBaseDriver dbDriver;

        private DataManager.HouseKeeping.FileZipAgent houseKeepingAgent;

        public static Logging.LogService logger;

        private bool initOK;
    
        public Receiver(String pathConfigFile)
        {
            this.pathConigFile = pathConfigFile;
            initOK = false;
        }

        public Receiver()
        {
            this.pathConigFile = @"C:\KTBDataManager\Programs\Config\KTBDataManagerServer.ini";
           
        }

        public bool init()
        {
            initOK = false;

            try
            {
                logger = Logging.LogService.Instance();
               

                if(!System.IO.File.Exists(pathConigFile))
                {
                    LogEntry(System.Diagnostics.EventLogEntryType.Error, "Receiver.init failed: " + pathConigFile + " does not exist");

                    return false;
                }

                Logging.IniFile ini = new Logging.IniFile(pathConigFile);

                DataBaseDriver.DBAddress = ini.Read("Address", "DATABASE");
                DataBaseDriver.DBName = ini.Read("DBName", "DATABASE");
                DataBaseDriver.DBUserName = ini.Read("DBUser", "DATABASE");
                DataBaseDriver.DBPassword = ini.Read("DBPwd", "DATABASE");

                listenPort = Int32.Parse(ini.Read("ListenPort", "SERVICE"));

                Logging.LogService.LogPath = ini.Read("LogPath", "PATH");

                String daysToZipLog = ini.Read("DaysToZip", "HOUSEKEEPING");
                String daysToDeleteLog = ini.Read("DaysToDelete", "HOUSEKEEPING");

                dbDriver = DataBaseDriver.Instance();

                tcpServer = new TCPServer(listenPort);
                tcpServer.OnClientMessage += handleDataMessage;

                houseKeepingAgent = new FileZipAgent(Logging.LogService.LogPath, daysToZipLog, daysToDeleteLog);

                if(!houseKeepingAgent.init())
                {
                    LogEntry(System.Diagnostics.EventLogEntryType.Error, "Receiver.init failed: Check paramaeter for housekeeping");

                    return false;
                }

                LogEntry(System.Diagnostics.EventLogEntryType.Information, "Receiver.init ok");

                initOK = true;

                return true;
            }
            catch(Exception ex)
            {
                LogEntry(System.Diagnostics.EventLogEntryType.Error, "Receiver.init failed: " + ex.Message);
            }

          
            return false;

        }

        public void start()
        {
            if(initOK)
            {
                houseKeepingAgent.Start();
                tcpServer.start();
            }

        }

        private String handleDataMessage(String message)
        {

            JMessage request = JsonConvert.DeserializeObject<JMessage>(message);

            JMessage replyMessage = new JMessage();
            replyMessage.Sender = "DataManagerService";

            // String to set to the InnerMessage
            String strInnerMessage = String.Empty;
          
            try
            {
                
                switch (request.Function)
                {
                    case "JEnterProductionDataLineRequest":
                        replyMessage.Function = "JEnterDataLineReply";
                        strInnerMessage = JsonConvert.SerializeObject(dbDriver.handleProductionDataLine(request));
                        break;
                    case "JEnterPowerUsageDataLineRequest":
                        replyMessage.Function = "JEnterDataLineReply";
                        strInnerMessage = JsonConvert.SerializeObject(dbDriver.handlePowerUsageLine(request));
                        break;
                    case "JSetAliveRequest":
                        replyMessage.Function = "JSetAliveResponse";
                        strInnerMessage = JsonConvert.SerializeObject(dbDriver.handleAliveMessage(request)); 
                        break;

                    default:
                        replyMessage.Function = "JEnterDataLineReply";
                        JEnterDataLineReply reply = new JEnterDataLineReply() { ErrorCode = 1, ErrorText = "Unvalid format", Success = false };
                        strInnerMessage = JsonConvert.SerializeObject(reply);
                        break;

                }
            }
            catch(Exception ex)
            {
                replyMessage.Function = "JEnterDataLineReply";
                JEnterDataLineReply reply = new JEnterDataLineReply() { ErrorCode = 1, ErrorText = "Unvalid format", Success = false };
                strInnerMessage = JsonConvert.SerializeObject(reply);

                LogEntry(System.Diagnostics.EventLogEntryType.Error, "Receiver.handleDataMessage failed: " + ex.Message);
                
            }

            replyMessage.InnerMessage = strInnerMessage;

            return JsonConvert.SerializeObject(replyMessage); 

        }

        public static void LogEntry(System.Diagnostics.EventLogEntryType type, String message)
        {
            logger.WriteEntry(type, message, "DataManagerService");
        }

    }
}
