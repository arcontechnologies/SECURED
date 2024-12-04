using System;
using System.Net;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Net.Http;
using System.Net.Http.Headers;
using DataEncryption;

namespace Rally
{
    class Helpers
    {
        // Compare 2 DataTables and output the findings
        public static DataTable GetSQLTable(string dbserver, string database, string TableName)
        {
            DataTable SQLTable = new DataTable();
            try
            {
                string connString = GetConnectionString(dbserver, database);
                string query = "SELECT COLUMN_NAME FROM DMAS.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'" + TableName + "'";

                SqlConnection conn = new SqlConnection(connString);
                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(SQLTable);
                conn.Close();
                da.Dispose();
            }
            catch (Exception e)
            {
 
                RallyLoad.logger.Error(e, "An error occurred in Get SQL Table: {0}");
            }
            return SQLTable;
        }

        public static DataTable GetAGTable(DataTable datatable)
        {
            DataTable columntable = new DataTable();
            try
            {
                string[] columnNames = (from dc in datatable.Columns.Cast<DataColumn>()
                                        select dc.ColumnName).ToArray();
                DataColumn NewCol;

                NewCol = new DataColumn
                {
                    ColumnName = "COLUMN_NAME",
                    DataType = System.Type.GetType("System.String")
                };
                columntable.Columns.Add(NewCol);

                DataRow NewRow = columntable.NewRow();
                foreach (var str in columnNames)
                {
                    columntable.Rows.Add(str);
                }
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error(e, "An error occurred in Get AG Table vs AG: {0}");
            }
            return columntable;
        }

        public static Tuple<bool, string> GetMappedColumn(string[] MappedConfiguration, string AGColumnName)
        {
            for (int i = 0; i < MappedConfiguration.Length; i++)
            {
                string MappedAGColumn = MappedConfiguration[i].Substring(MappedConfiguration[i].LastIndexOf(':') + 1);
                if (MappedAGColumn == AGColumnName)
                {
                    string MappedSQLColumn = MappedConfiguration[i].Substring(0, MappedConfiguration[i].IndexOf(':'));
                    return Tuple.Create(true, MappedSQLColumn);
                }
            }
            return Tuple.Create(false, "");
        }

        public static DataTable CompareRows(DataTable SQLtable, DataTable AGtable, DataTable Resulttable, string[] listmappedcolumn)
        {
            try
            {

                Tuple<bool, string> MappedColumn;

                for (int i = AGtable.Rows.Count - 1; i >= 0; i--)
                {
                    bool isfound = false;
                    foreach (DataRow SQLrow in SQLtable.Rows)
                    {
                        var SQLarray = SQLrow.ItemArray;
                        string CurrentColumnValue = AGtable.Rows[i]["COLUMN_NAME"].ToString();

                        if (SQLarray.Contains(CurrentColumnValue))
                        {
                            isfound = true;
                            break;
                        }
                    }

                    if (!isfound)
                    {
                        // case where column should be mapped to a new column

                        MappedColumn = GetMappedColumn(listmappedcolumn, AGtable.Rows[i]["COLUMN_NAME"].ToString());

                        if (MappedColumn.Item1 == true)
                        {
                            Resulttable.Columns[i].ColumnName = MappedColumn.Item2.ToString();
                        }
                        else
                        {
                            Resulttable.Columns.Remove(Resulttable.Columns[i]);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error(e, "An error occurred in Compare Rows SQL table vs AG: {0}");
            }
            return Resulttable;
        }

        // Connection string to the db Server and related database declacred in app.config
        public static string GetConnectionString(string dbserver, string database)
        {
            string ConnectionString = string.Empty;
            try
            {
                ConnectionString = @"Data Source=" + dbserver + ";Integrated Security=true;Initial Catalog=" + database + ";";
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error(e, "An error occurred in get connection: {0}");
            }
            return ConnectionString;
        }


        // method to load configuration data from Configtable declared in app.config
        public static DataTable LoadConfiguration(string dbserver, string database, string configtable)
        {
            DataTable datatable = new DataTable();

            try
            {
                string connString = GetConnectionString(dbserver, database);
                string query = "select * from " + configtable;

                SqlConnection conn = new SqlConnection(connString);
                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(datatable);
                conn.Close();
                da.Dispose();

            }
            catch (Exception e)
            {
                RallyLoad.logger.Error(e, "An error occurred in load configuration : {0}");
            }
            return datatable;
        }


        // given a Category and a Key, this function returns the related value.
        public static string ReadConfiguration(DataTable datatable, string category, string key)
        {
            DataRow[] value;
            string expression = "Category Like '" + category + "' and Key Like '" + key + "'";

            value = datatable.Select(expression);
            return value[0][3].ToString();
        }

        // given a Category and a Key, this function returns the list of related value but only for those who are enabled.
        public static string[] ReadListConfiguration(DataTable datatable, string category, string key)
        {
            DataRow[] value;

            value = datatable.Select("Category Like '" + category + "' and Key Like '" + key + "' and Enabled = 1");

            string[] result = new string[value.Length];
            int i = 0;

            try
            {

                foreach (var dr in value)
                {
                    result[i] = value[i][3].ToString();
                    i++;
                }
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error(e, "An error occurred in list configuration : {0}");
            }
            return result;

        }

        public static HttpClient WebAuthenticationWithToken(string connectionString, string app)
        {
            HttpClientHandler handler = new HttpClientHandler()
            {
                SslProtocols = System.Security.Authentication.SslProtocols.Tls12
            };

            HttpClient confClient = new HttpClient(handler);

            try
            {
                string decryptedToken = EncryptionService.ReadEncryptedDataFromDatabase(connectionString, app);
                confClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + decryptedToken);
                confClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                confClient.Timeout = TimeSpan.FromMilliseconds(100000000);

            }
            catch (WebException e)
            {
                RallyLoad.logger.Error(e.Message, "An error occurred in HTTP request (web exception) : {0}- trygain ");
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error(e.Message, "An error occurred in HTTP request (other exception) : {0}- trygain ");
            }
            return confClient;
        }

        public static string WebRequestWithToken(HttpClient confClient, string url)
        {
            string json = string.Empty;
            bool tryagain = true;
            int nbRetry = Convert.ToInt32(ConfigurationManager.AppSettings["nbRetry"].ToString());

            while (tryagain)
            {
                try
                {
                    if (nbRetry > 0)
                    {
                        HttpResponseMessage message = confClient.GetAsync(url).Result;

                        if (message.IsSuccessStatusCode)
                        {
                            var inter = message.Content.ReadAsStringAsync();
                            json = inter.Result;

                            json.Replace("\"", @"""");
                            if (json.TrimStart().StartsWith("<") == false)
                            {
                                tryagain = false;
                                return json;
                            }
                        }
                        else
                        {
                            Console.WriteLine(message.ReasonPhrase);
                            RallyLoad.logger.Error(message.ReasonPhrase);
                            nbRetry--;
                        }
                    }
                    else
                    {
                        Console.WriteLine("Http Response error - The number of retries has been reached. The program is stopped");
                        RallyLoad.logger.Error("Http Response error - The number of retries has been reached. The program is stopped");
                        Environment.Exit(-1);
                    }
                }
                catch (Exception e)
                {
                    if (nbRetry > 0)
                    {
                        Console.WriteLine("Http Response error : {0} - retry number : {1}", e.Message, nbRetry);
                        RallyLoad.logger.Error("Http Response error - retry number : " + nbRetry.ToString() + " " + e.Message);
                        nbRetry--;
                    }
                    else
                    {
                        Console.WriteLine("Http Response error - The number of retries has been reached. The program is stopped");
                        RallyLoad.logger.Error("Http Response error - The number of retries has been reached. The program is stopped");
                        Environment.Exit(-1);
                    }
                }
            }
            return json;
        }
    }
}
