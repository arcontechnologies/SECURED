using System;
using System.Net;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
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
                var builder = new SqlConnectionStringBuilder
                {
                    DataSource = dbserver,
                    InitialCatalog = database,
                    IntegratedSecurity = true
                };

                using (SqlConnection conn = new SqlConnection(builder.ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.Add(new SqlParameter("@TableName", SqlDbType.NVarChar, 128) { Value = TableName });
                        cmd.CommandText = "SELECT COLUMN_NAME FROM DMAS.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TableName";
                        
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
                RallyLoad.logger.Error("An error occurred in Get SQL Table", e);
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

                columntable.Columns.Add(new DataColumn
                {
                    ColumnName = "COLUMN_NAME",
                    DataType = typeof(string)
                });

                foreach (var str in columnNames)
                {
                    columntable.Rows.Add(str);
                }
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error("An error occurred in Get AG Table vs AG", e);
            }
            return columntable;
        }

        public static Tuple<bool, string> GetMappedColumn(string[] MappedConfiguration, string AGColumnName)
        {
            if (MappedConfiguration == null || string.IsNullOrWhiteSpace(AGColumnName))
            {
                return Tuple.Create(false, string.Empty);
            }

            for (int i = 0; i < MappedConfiguration.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(MappedConfiguration[i])) continue;

                int colonIndex = MappedConfiguration[i].IndexOf(':');
                if (colonIndex == -1) continue;

                string MappedAGColumn = MappedConfiguration[i].Substring(colonIndex + 1);
                if (MappedAGColumn.Equals(AGColumnName, StringComparison.OrdinalIgnoreCase))
                {
                    string MappedSQLColumn = MappedConfiguration[i].Substring(0, colonIndex);
                    return Tuple.Create(true, MappedSQLColumn);
                }
            }
            return Tuple.Create(false, string.Empty);
        }

        public static DataTable CompareRows(DataTable SQLtable, DataTable AGtable, DataTable Resulttable, string[] listmappedcolumn)
        {
            try
            {
                for (int i = AGtable.Rows.Count - 1; i >= 0; i--)
                {
                    bool isfound = false;
                    string CurrentColumnValue = AGtable.Rows[i]["COLUMN_NAME"]?.ToString();
                    
                    if (string.IsNullOrEmpty(CurrentColumnValue)) continue;

                    foreach (DataRow SQLrow in SQLtable.Rows)
                    {
                        if (SQLrow.ItemArray.Contains(CurrentColumnValue))
                        {
                            isfound = true;
                            break;
                        }
                    }

                    if (!isfound)
                    {
                        var MappedColumn = GetMappedColumn(listmappedcolumn, CurrentColumnValue);
                        if (MappedColumn.Item1)
                        {
                            Resulttable.Columns[i].ColumnName = MappedColumn.Item2;
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
                RallyLoad.logger.Error("An error occurred in Compare Rows SQL table vs AG", e);
            }
            return Resulttable;
        }

        public static string GetConnectionString(string dbserver, string database)
        {
            try
            {
                var builder = new SqlConnectionStringBuilder
                {
                    DataSource = dbserver,
                    InitialCatalog = database,
                    IntegratedSecurity = true
                };
                return builder.ConnectionString;
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error("An error occurred in get connection", e);
                return string.Empty;
            }
        }

        public static DataTable LoadConfiguration(string dbserver, string database, string configtable)
        {
            DataTable datatable = new DataTable();
            try
            {
                var builder = new SqlConnectionStringBuilder
                {
                    DataSource = dbserver,
                    InitialCatalog = database,
                    IntegratedSecurity = true
                };

                using (SqlConnection conn = new SqlConnection(builder.ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.Add(new SqlParameter("@configtable", SqlDbType.NVarChar, 128) { Value = configtable });
                        cmd.CommandText = "SELECT * FROM @configtable";
                        
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
                RallyLoad.logger.Error("An error occurred in load configuration", e);
            }
            return datatable;
        }

        public static string ReadConfiguration(DataTable datatable, string category, string key)
        {
            try
            {
                var parameters = new List<SqlParameter>
                {
                    new SqlParameter("@category", SqlDbType.NVarChar, 50) { Value = category },
                    new SqlParameter("@key", SqlDbType.NVarChar, 50) { Value = key }
                };

                string expression = "Category = @category AND Key = @key";
                DataRow[] value = datatable.Select(expression);
                return value.Length > 0 ? value[0][3].ToString() : string.Empty;
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error("An error occurred in read configuration", e);
                return string.Empty;
            }
        }

        public static string[] ReadListConfiguration(DataTable datatable, string category, string key)
        {
            try
            {
                var parameters = new List<SqlParameter>
                {
                    new SqlParameter("@category", SqlDbType.NVarChar, 50) { Value = category },
                    new SqlParameter("@key", SqlDbType.NVarChar, 50) { Value = key }
                };

                string expression = "Category = @category AND Key = @key AND Enabled = 1";
                DataRow[] value = datatable.Select(expression);

                string[] result = new string[value.Length];
                for (int i = 0; i < value.Length; i++)
                {
                    result[i] = value[i][3].ToString();
                }
                return result;
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error("An error occurred in list configuration", e);
                return Array.Empty<string>();
            }
        }

        public static HttpClient WebAuthenticationWithToken(string connectionString, string app)
        {
            var handler = new HttpClientHandler
            {
                SslProtocols = System.Security.Authentication.SslProtocols.Tls12
            };

            var confClient = new HttpClient(handler);

            try
            {
                string decryptedToken = EncryptionService.ReadEncryptedDataFromDatabase(connectionString, app);
                if (!string.IsNullOrEmpty(decryptedToken))
                {
                    confClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", decryptedToken);
                    confClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    confClient.Timeout = TimeSpan.FromMilliseconds(100000000);
                }
            }
            catch (WebException e)
            {
                RallyLoad.logger.Error("An error occurred in HTTP request (web exception)", e);
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error("An error occurred in HTTP request (other exception)", e);
            }
            return confClient;
        }

        public static string WebRequestWithToken(HttpClient confClient, string url)
        {
            string json = string.Empty;
            bool tryagain = true;
            int nbRetry = Convert.ToInt32(ConfigurationManager.AppSettings["nbRetry"]);
            int remainingRetries = nbRetry;

            while (tryagain && remainingRetries > 0)
            {
                try
                {
                    HttpResponseMessage message = confClient.GetAsync(url).Result;

                    if (message.IsSuccessStatusCode)
                    {
                        var inter = message.Content.ReadAsStringAsync();
                        json = inter.Result;

                        if (!string.IsNullOrEmpty(json))
                        {
                            json = json.Replace("\"", @"""");
                            if (!json.TrimStart().StartsWith("<"))
                            {
                                tryagain = false;
                                return json;
                            }
                        }
                    }
                    else
                    {
                        RallyLoad.logger.Error($"HTTP Response error: {message.ReasonPhrase}");
                        remainingRetries--;
                    }
                }
                catch (Exception e)
                {
                    RallyLoad.logger.Error($"HTTP Response error - retry number: {nbRetry - remainingRetries}", e);
                    remainingRetries--;
                }
            }

            if (remainingRetries <= 0)
            {
                RallyLoad.logger.Error("HTTP Response error - The number of retries has been reached. The program is stopped");
                Environment.Exit(-1);
            }

            return json;
        }
    }
}
