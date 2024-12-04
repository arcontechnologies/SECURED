using System;
using System.Net;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security;
using DataEncryption;

namespace Rally
{
    class Helpers
    {
        public static DataTable GetSQLTable(string dbserver, string database, string TableName)
        {
            DataTable SQLTable = new DataTable();
            try
            {
                using (SqlConnection conn = new SqlConnection())
                {
                    conn.ConnectionString = ConfigurationManager.ConnectionStrings["RallyDB"].ConnectionString;
                    using (SqlCommand cmd = new SqlCommand("SELECT COLUMN_NAME FROM DMAS.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TableName", conn))
                    {
                        cmd.Parameters.AddWithValue("@TableName", TableName);
                        conn.Open();
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(SQLTable);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error(new SecurityException("SQL Table Error"), "An error occurred in Get SQL Table: {0}");
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
                DataColumn NewCol = new DataColumn
                {
                    ColumnName = "COLUMN_NAME",
                    DataType = System.Type.GetType("System.String")
                };
                columntable.Columns.Add(NewCol);

                foreach (var str in columnNames)
                {
                    columntable.Rows.Add(str);
                }
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error(new SecurityException("AG Table Error"), "An error occurred in Get AG Table vs AG: {0}");
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
                        MappedColumn = GetMappedColumn(listmappedcolumn, AGtable.Rows[i]["COLUMN_NAME"].ToString());
                        if (MappedColumn.Item1)
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
                RallyLoad.logger.Error(new SecurityException("Compare Rows Error"), "An error occurred in Compare Rows SQL table vs AG: {0}");
            }
            return Resulttable;
        }

        public static string GetConnectionString(string dbserver, string database)
        {
            return ConfigurationManager.ConnectionStrings["RallyDB"].ConnectionString;
        }

        public static DataTable LoadConfiguration(string dbserver, string database, string configtable)
        {
            DataTable datatable = new DataTable();
            try
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["RallyDB"].ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand("SELECT * FROM @ConfigTable", conn))
                    {
                        cmd.Parameters.AddWithValue("@ConfigTable", configtable);
                        conn.Open();
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(datatable);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error(new SecurityException("Load Configuration Error"), "An error occurred in load configuration : {0}");
            }
            return datatable;
        }

        public static string ReadConfiguration(DataTable datatable, string category, string key)
        {
            using (SqlCommand cmd = new SqlCommand())
            {
                cmd.Parameters.AddWithValue("@Category", category);
                cmd.Parameters.AddWithValue("@Key", key);
                DataRow[] value = datatable.Select("Category = @Category AND Key = @Key");
                return value[0][3].ToString();
            }
        }

        public static string[] ReadListConfiguration(DataTable datatable, string category, string key)
        {
            using (SqlCommand cmd = new SqlCommand())
            {
                cmd.Parameters.AddWithValue("@Category", category);
                cmd.Parameters.AddWithValue("@Key", key);
                DataRow[] value = datatable.Select("Category = @Category AND Key = @Key AND Enabled = 1");

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
                    RallyLoad.logger.Error(new SecurityException("List Configuration Error"), "An error occurred in list configuration : {0}");
                }
                return result;
            }
        }

        public static HttpClient WebAuthenticationWithToken(string connectionString, string app)
        {
            var handler = new HttpClientHandler()
            {
                SslProtocols = System.Security.Authentication.SslProtocols.Tls12
            };

            var confClient = new HttpClient(handler);
            try
            {
                string decryptedToken = EncryptionService.ReadEncryptedDataFromDatabase(
                    ConfigurationManager.ConnectionStrings["RallyDB"].ConnectionString, 
                    app);
                confClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", decryptedToken);
                confClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                confClient.Timeout = TimeSpan.FromMilliseconds(100000000);
            }
            catch (WebException e)
            {
                RallyLoad.logger.Error(new SecurityException("Web Authentication Error"), "An error occurred in HTTP request : {0}");
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error(new SecurityException("Authentication Error"), "An error occurred in HTTP request : {0}");
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
                            json = SecurityElement.Escape(json);

                            if (!json.TrimStart().StartsWith("<"))
                            {
                                tryagain = false;
                                return json;
                            }
                        }
                        else
                        {
                            RallyLoad.logger.Error(new SecurityException("Web Request Error"), SecurityElement.Escape(message.ReasonPhrase));
                            nbRetry--;
                        }
                    }
                    else
                    {
                        RallyLoad.logger.Error(new SecurityException("Web Request Retry Error"), "Http Response error - The number of retries has been reached.");
                        Environment.Exit(-1);
                    }
                }
                catch (Exception e)
                {
                    if (nbRetry > 0)
                    {
                        RallyLoad.logger.Error(new SecurityException("Web Request Exception"), 
                            SecurityElement.Escape($"Http Response error - retry number : {nbRetry}"));
                        nbRetry--;
                    }
                    else
                    {
                        RallyLoad.logger.Error(new SecurityException("Web Request Final Error"), 
                            "Http Response error - The number of retries has been reached.");
                        Environment.Exit(-1);
                    }
                }
            }
            return json;
        }
    }
}
