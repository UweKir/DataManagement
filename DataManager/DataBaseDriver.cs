using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data.Sql;
using Newtonsoft.Json;
using DataManager.Network.JSON;
using System.Data;


namespace DataManager
{
    public class DataBaseDriver
    {
        #region Class Members

        private static DataBaseDriver instance = null;

        public static String DBName { get; set; }

        public static String DBAddress { get; set; }

        public static String DBUserName { get; set; }

        public static String DBPassword { get; set; }

        private SqlConnectionStringBuilder connStringBuilder;

        

        #endregion

        private DataBaseDriver()
        {         
            setConnection();
        }

        public static DataBaseDriver Instance()
        {
            if (instance == null)
                instance = new DataBaseDriver();

            return instance;
        }

        private void setConnection()
        {
            connStringBuilder = new SqlConnectionStringBuilder();
            connStringBuilder.DataSource = DBAddress;
            connStringBuilder.InitialCatalog = DBName;
            connStringBuilder.UserID = DBUserName;
            connStringBuilder.Password = DBPassword;

            checkConnection();
        }

        private bool checkConnection()
        {
            SqlConnection conn = new SqlConnection(connStringBuilder.ConnectionString);

            try
            {
                conn.Open();
                Receiver.LogEntry(System.Diagnostics.EventLogEntryType.Information, "DataBaseDriver.checkConnection success");

                return true;
            }
            catch(Exception ex)
            {
                Receiver.LogEntry(System.Diagnostics.EventLogEntryType.Error, "DataBaseDriver.checkConnection failed: " + ex.Message);

                return false;
            }
            finally
            {
                conn.Close();
            }

        }

        public JEnterDataLineReply handleProductionDataLine(JMessage request)
        {

            SqlConnection conn = new SqlConnection(connStringBuilder.ConnectionString);

            JEnterProductionDataLineRequest prodLine = JsonConvert.DeserializeObject<JEnterProductionDataLineRequest>(request.InnerMessage);

            JEnterDataLineReply reply = new JEnterDataLineReply();
            reply.ErrorCode = 0;
            reply.Success = true;
            reply.ErrorText = "OK";

            try
            {

                for (int i = 0; i < prodLine.DataColumns.Count; i++)
                {
                    JDataValue dataValue = prodLine.DataColumns[i];

                    if (!rowExists(request, prodLine, dataValue, conn))
                    {
                        if (!insertRow(request, prodLine, dataValue, conn))
                        {
                            reply.Success = false;
                            reply.ErrorCode = 300;
                            reply.ErrorText = "Could not insert line";

                            break;
                        }
                    }
                    else
                    {
                        Receiver.LogEntry(System.Diagnostics.EventLogEntryType.Warning, "DataBaseDriver.handleProductionLine Entry already exists for value " + prodLine.SourceFile + " Line " + prodLine.LineNumber + " value " + dataValue.Value.ToString());
                        Console.WriteLine("Entry already exists for " + prodLine.SourceFile + " Line " + prodLine.LineNumber + " value " + dataValue.Value.ToString());
                    }
                }
            }
            catch(Exception ex)
            {
                Receiver.LogEntry(System.Diagnostics.EventLogEntryType.Error, "DataBaseDriver.handleProductionLine failed: " + ex.Message);

                reply.Success = false;
                reply.ErrorCode = 301;
                reply.ErrorText = "DB Error";
            }
            finally
            {
               conn.Close();
            }

            
            if (reply.Success)
            {
                Receiver.LogEntry(System.Diagnostics.EventLogEntryType.SuccessAudit, "DataBaseDriver.handleProductionLineRequest success for " + prodLine.SourceFile + " Line " + prodLine.LineNumber);
                Console.WriteLine("Success for " + prodLine.SourceFile + " Line " + prodLine.LineNumber);
            }

           return reply;

        }

        public JEnterDataLineReply handlePowerUsageLine(JMessage request)
        {
            SqlConnection conn = new SqlConnection(connStringBuilder.ConnectionString);

            JEnterPowerUsageDataLineRequest powerLine = JsonConvert.DeserializeObject<JEnterPowerUsageDataLineRequest>(request.InnerMessage);

            JEnterDataLineReply reply = new JEnterDataLineReply();
            reply.ErrorCode = 0;
            reply.Success = true;
            reply.ErrorText = "OK";

            try
            {

                for (int i = 0; i < powerLine.DataColumns.Count; i++)
                {
                    JDataValue dataValue = powerLine.DataColumns[i];

                    if (!rowExists(request, powerLine, dataValue, conn))
                    {
                        if (!insertRow(request, powerLine, dataValue, conn))
                        {
                            reply.Success = false;
                            reply.ErrorCode = 300;
                            reply.ErrorText = "Could not insert line";

                            break;
                        }
                    }
                    else
                    {
                        Receiver.LogEntry(System.Diagnostics.EventLogEntryType.Warning, "DataBaseDriver.handlePowerUsageLine Entry already exists for value " + powerLine.SourceFile + " Line " + powerLine.LineNumber + " value " + dataValue.Value.ToString());
                        Console.WriteLine("Entry already exists for " + powerLine.SourceFile + " Line " + powerLine.LineNumber + " value " + dataValue.Value.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                Receiver.LogEntry(System.Diagnostics.EventLogEntryType.Error, "DataBaseDriver.handlePowerUsageLine failed: " + ex.Message);

                reply.Success = false;
                reply.ErrorCode = 301;
                reply.ErrorText = "DB Error";
            }
            finally
            {
                conn.Close();
            }

            if (reply.Success)
            {
                Receiver.LogEntry(System.Diagnostics.EventLogEntryType.SuccessAudit, "DataBaseDriver.handlePowerUsageLine success for " + powerLine.SourceFile + " Line " + powerLine.LineNumber);
                Console.WriteLine("Success for " + powerLine.SourceFile + " Line " + powerLine.LineNumber);
            }


            return reply;
        }

        public JSetAliveResponse handleAliveMessage(JMessage request)
        {
            SqlConnection conn = new SqlConnection(connStringBuilder.ConnectionString);

            JSetAliveRequest jSetAliveRequest = JsonConvert.DeserializeObject<JSetAliveRequest>(request.InnerMessage);

            JSetAliveResponse jSetAliveResponse = new JSetAliveResponse();
            jSetAliveResponse.Ack = false;
            jSetAliveResponse.DateAck = DateTime.Now;

            try
            {
                if (insertRow(request, jSetAliveRequest, conn))
                {
                    jSetAliveResponse.Ack = true;

                    Receiver.LogEntry(System.Diagnostics.EventLogEntryType.Information, "DataBaseDriver.handleAliveMessage success for location " + request.Sender);
                    Console.WriteLine("DataBaseDriver.handleAliveMessage success for location " + request.Sender);

                }
                else
                {
                    Receiver.LogEntry(System.Diagnostics.EventLogEntryType.Error, "DataBaseDriver.handleAliveMessage failed for location " + request.Sender);
                    Console.WriteLine("DataBaseDriver.handleAliveMessage failed for location " + request.Sender);
                }            
            }
            catch (Exception ex)
            {
                Receiver.LogEntry(System.Diagnostics.EventLogEntryType.Error, "DataBaseDriver.handleAliveMessage failed for location " + request.Sender + " " + ex.Message);
                Console.WriteLine("DataBaseDriver.handleAliveMessage failed for location " + request.Sender + " " + ex.Message);
            }
            finally
            {
                conn.Close();
            }

            return jSetAliveResponse;

        }

        #region Database functions

        #region Inserts

        private bool insertRow(JMessage request, JEnterPowerUsageDataLineRequest prodLine, JDataValue dataValue, SqlConnection conn)
        {


            DataSet ds = new DataSet("ConsumerCounter");

            try
            {


                decimal counterValue = Decimal.Parse(dataValue.Value);


                using (SqlCommand command = new SqlCommand("spAddConsumerCounter", conn))
                {
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    command.Parameters.Add(new SqlParameter("@DATE", prodLine.dtCreated));
                    command.Parameters.Add(new SqlParameter("@PROCESSING_DATE", DateTime.Now));
                    command.Parameters.Add(new SqlParameter("@COUNTER_VALUE", counterValue));
                    command.Parameters.Add(new SqlParameter("@LOCATION_NAME", request.Sender));
                    command.Parameters.Add(new SqlParameter("@UNIT_NAME", dataValue.Unity));
                    command.Parameters.Add(new SqlParameter("@CONSUMPTION_TYPE_NAME", dataValue.Article));
                    command.Parameters.Add(new SqlParameter("@DEVICE_NAME", dataValue.Device));
                    command.Parameters.Add(new SqlParameter("@SOURCE", prodLine.SourceFile));
                    command.Parameters.Add(new SqlParameter("@SOURCE_INDEX", prodLine.LineNumber));


                    if (conn.State != System.Data.ConnectionState.Open)
                        conn.Open();

                    SqlDataAdapter da = new SqlDataAdapter();
                    da.SelectCommand = command;

                    da.Fill(ds);

                }

                // Entry does not exist, insert the row to DB
                if (ds.Tables[0].Rows.Count == 0)
                {
                    return false;
                }

                return true;

            }
            catch (Exception ex)
            {
                Receiver.LogEntry(System.Diagnostics.EventLogEntryType.Error, "DataBaseDriver.insertRow failed for PowerUsageLine: " + ex.Message);
                return false;
            }

        }

        private bool insertRow(JMessage request, JEnterProductionDataLineRequest prodLine, JDataValue dataValue, SqlConnection conn)
        {
            

            DataSet ds = new DataSet("ScaleCounter");

            try
            {


                decimal counterValue = Decimal.Parse(dataValue.Value);


                using (SqlCommand command = new SqlCommand("spAddScaleCounter", conn))
                {
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    command.Parameters.Add(new SqlParameter("@DATE", prodLine.dtCreated));
                    command.Parameters.Add(new SqlParameter("@PROCESSING_DATE", DateTime.Now));
                    command.Parameters.Add(new SqlParameter("@COUNTER_VALUE", counterValue));
                    command.Parameters.Add(new SqlParameter("@LOCATION_NAME", request.Sender));
                    command.Parameters.Add(new SqlParameter("@UNIT_NAME", dataValue.Unity));
                    command.Parameters.Add(new SqlParameter("@ARTICLE_NAME", dataValue.Article));
                    command.Parameters.Add(new SqlParameter("@DEVICE_NAME", dataValue.Device));
                    command.Parameters.Add(new SqlParameter("@SOURCE", prodLine.SourceFile));
                    command.Parameters.Add(new SqlParameter("@SOURCE_INDEX", prodLine.LineNumber));


                    if (conn.State != System.Data.ConnectionState.Open)
                        conn.Open();

                    SqlDataAdapter da = new SqlDataAdapter();
                    da.SelectCommand = command;

                    da.Fill(ds);

                }

                // Entry does not exist, insert the row to DB
                if (ds.Tables[0].Rows.Count == 0)
                {
                    return false;
                }

                return true;

            }
            catch(Exception ex)
            {
                Receiver.LogEntry(System.Diagnostics.EventLogEntryType.Error, "DataBaseDriver.insertRow for ProductionDataLine failed: " + ex.Message);
                return false;
            }

        }

        private bool insertRow(JMessage request, JSetAliveRequest jSetAliveRequest, SqlConnection conn)
        {
            DataSet ds = new DataSet("Location");

            try
            {

                using (SqlCommand command = new SqlCommand("spSetAliveToLocation", conn))
                {
                    command.CommandType = System.Data.CommandType.StoredProcedure;                    
                    command.Parameters.Add(new SqlParameter("@LOCATION_NAME", request.Sender));
                   
                    if (conn.State != System.Data.ConnectionState.Open)
                        conn.Open();

                    SqlDataAdapter da = new SqlDataAdapter();
                    da.SelectCommand = command;

                    da.Fill(ds);

                }

                // Entry does not exist, insert the row to DB
                if (ds.Tables[0].Rows.Count == 0)
                {
                    return false;
                }

                return true;

            }
            catch (Exception ex)
            {
                Receiver.LogEntry(System.Diagnostics.EventLogEntryType.Error, "DataBaseDriver.insertRow for ProductionDataLine failed: " + ex.Message);
                return false;
            }
        }

        #endregion

        #region Row exists

        /// <summary>
        /// Checks if entry already exists in DB
        /// </summary>
        /// <param name="request"></param>
        /// <param name="powerUsageLine"></param>
        /// <param name="dataValue"></param>
        /// <returns>bool entry exists</returns>
        private bool rowExists(JMessage request, JEnterPowerUsageDataLineRequest powerUsageLine, JDataValue dataValue, SqlConnection conn)
        {

            DataSet ds = new DataSet("ConsumerCounter");

            using (SqlCommand command = new SqlCommand("spGetSingleConsumerCounter", conn))
            {
                command.CommandType = System.Data.CommandType.StoredProcedure;
                command.Parameters.Add(new SqlParameter("@DATE", powerUsageLine.dtCreated));
                command.Parameters.Add(new SqlParameter("@CONSUMPTION_TYPE_NAME", dataValue.Article));
                command.Parameters.Add(new SqlParameter("@UNIT_NAME", dataValue.Unity));
                command.Parameters.Add(new SqlParameter("@DEVICE_NAME", dataValue.Device));
                command.Parameters.Add(new SqlParameter("@LOCATION_NAME", request.Sender));

                if (conn.State != System.Data.ConnectionState.Open)
                    conn.Open();

                SqlDataAdapter da = new SqlDataAdapter();
                da.SelectCommand = command;

                da.Fill(ds);

            }

            // Entry does not exist, insert the row to DB
            if (ds.Tables[0].Rows.Count == 0)
            {
                return false;
            }


            return true;
        }

        /// <summary>
        /// Checks if entry already exists in DB
        /// </summary>
        /// <param name="request"></param>
        /// <param name="prodLine"></param>
        /// <param name="dataValue"></param>
        /// <returns>bool entry exists</returns>
        private bool rowExists(JMessage request, JEnterProductionDataLineRequest prodLine, JDataValue dataValue, SqlConnection conn)
        {

            DataSet ds = new DataSet("ScaleCounter");

            using (SqlCommand command = new SqlCommand("spGetSingleScaleCounter", conn))
            {
                command.CommandType = System.Data.CommandType.StoredProcedure;
                command.Parameters.Add(new SqlParameter("@DATE", prodLine.dtCreated));
                command.Parameters.Add(new SqlParameter("@ARTICLE_NAME", dataValue.Article));
                command.Parameters.Add(new SqlParameter("@UNIT_NAME", dataValue.Unity));
                command.Parameters.Add(new SqlParameter("@DEVICE_NAME", dataValue.Device));
                command.Parameters.Add(new SqlParameter("@LOCATION_NAME", request.Sender));

                if (conn.State != System.Data.ConnectionState.Open)
                    conn.Open();

                SqlDataAdapter da = new SqlDataAdapter();
                da.SelectCommand = command;

                da.Fill(ds);

            }

            // Entry does not exist, insert the row to DB
            if (ds.Tables[0].Rows.Count == 0)
            {
                return false;
            }


            return true;
        }

        #endregion

        #endregion
    }

}
