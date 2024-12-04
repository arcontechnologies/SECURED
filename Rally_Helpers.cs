using System;
using System.Net;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security;
using System.Web;
using DataEncryption;

namespace Rally
{
    class Helpers
    {
        // Compare 2 DataTables and output the findings
        public static DataTable GetSQLTable(string dbserver, string database, string tableName)
        {
            DataTable sqlTable = new DataTable();
            try
            {
                // Use parameterized connection string with sanitized inputs
                using (var conn = new SqlConnection(BuildSecureConnectionString(dbserver, database)))
                using (var cmd = new SqlCommand("SELECT COLUMN_NAME FROM DMAS.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TableName", conn))
                {
                    cmd.Parameters.AddWithValue("@TableName", tableName);
                    conn.Open();
                    using (var da = new SqlDataAdapter(cmd))
                    {
                        da.Fill(sqlTable);
                    }
                }
            }
            catch (Exception e)
            {
                // Use sanitized error logging
                RallyLoad.logger.Error("An error occurred in Get SQL Table: {0}", 
                    SecurityElement.Escape(e.Message));
            }
            return sqlTable;
        }

        public static DataTable GetAGTable(DataTable datatable)
        {
            DataTable columnTable = new DataTable();
            try
            {
                string[] columnNames = (from dc in datatable.Columns.Cast<DataColumn>()
                                      select dc.ColumnName).ToArray();

                var newCol = new DataColumn
                {
                    ColumnName = "COLUMN_NAME",
                    DataType = typeof(string)
                };
                columnTable.Columns.Add(newCol);

                foreach (var str in columnNames)
                {
                    columnTable.Rows.Add(str);
                }
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error("An error occurred in Get AG Table vs AG: {0}", 
                    SecurityElement.Escape(e.Message));
            }
            return columnTable;
        }

        public static Tuple<bool, string> GetMappedColumn(string[] mappedConfiguration, string agColumnName)
        {
            if (mappedConfiguration == null || string.IsNullOrEmpty(agColumnName))
                return Tuple.Create(false, string.Empty);

            foreach (var config in mappedConfiguration)
            {
                var lastColonIndex = config.LastIndexOf(':');
                if (lastColonIndex == -1) continue;

                string mappedAGColumn = config.Substring(lastColonIndex + 1);
                if (mappedAGColumn == agColumnName)
                {
                    string mappedSQLColumn = config.Substring(0, config.IndexOf(':'));
                    return Tuple.Create(true, mappedSQLColumn);
                }
            }
            return Tuple.Create(false, string.Empty);
        }

        public static DataTable CompareRows(DataTable sqlTable, DataTable agTable, 
            DataTable resultTable, string[] listMappedColumn)
        {
            try
            {
                for (int i = agTable.Rows.Count - 1; i >= 0; i--)
                {
                    bool isFound = false;
                    string currentColumnValue = agTable.Rows[i]["COLUMN_NAME"].ToString();

                    foreach (DataRow sqlRow in sqlTable.Rows)
                    {
                        if (sqlRow.ItemArray.Contains(currentColumnValue))
                        {
                            isFound = true;
                            break;
                        }
                    }

                    if (!isFound)
                    {
                        var mappedColumn = GetMappedColumn(listMappedColumn, 
                            agTable.Rows[i]["COLUMN_NAME"].ToString());

                        if (mappedColumn.Item1)
                        {
                            resultTable.Columns[i].ColumnName = mappedColumn.Item2;
                        }
                        else
                        {
                            resultTable.Columns.Remove(resultTable.Columns[i]);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error("An error occurred in Compare Rows SQL table vs AG: {0}", 
                    SecurityElement.Escape(e.Message));
            }
            return resultTable;
        }

        // Secure connection string builder
        private static string BuildSecureConnectionString(string dbServer, string database)
        {
            if (string.IsNullOrEmpty(dbServer) || string.IsNullOrEmpty(database))
                throw new ArgumentException("Database connection parameters cannot be null or empty");

            // Sanitize inputs
            dbServer = SecurityElement.Escape(dbServer);
            database = SecurityElement.Escape(database);

            var builder = new SqlConnectionStringBuilder
            {
                DataSource = dbServer,
                InitialCatalog = database,
                IntegratedSecurity = true,
                MultipleActiveResultSets = false,
                Encrypt = true,
                TrustServerCertificate = false,
                ConnectTimeout = 30
            };

            return builder.ConnectionString;
        }

        // Secure method to load configuration data
        public static DataTable LoadConfiguration(string dbServer, string database, string configTable)
        {
            var dataTable = new DataTable();

            try
            {
                using (var conn = new SqlConnection(BuildSecureConnectionString(dbServer, database)))
                using (var cmd = new SqlCommand("SELECT * FROM @ConfigTable", conn))
                {
                    cmd.Parameters.AddWithValue("@ConfigTable", configTable);
                    conn.Open();
                    using (var da = new SqlDataAdapter(cmd))
                    {
                        da.Fill(dataTable);
                    }
                }
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error("An error occurred in load configuration: {0}", 
                    SecurityElement.Escape(e.Message));
            }
            return dataTable;
        }

        // Secure configuration reader
        public static string ReadConfiguration(DataTable dataTable, string category, string key)
        {
            if (dataTable == null || string.IsNullOrEmpty(category) || string.IsNullOrEmpty(key))
                return string.Empty;

            try
            {
                var expression = "Category = @category AND Key = @key";
                var rows = dataTable.Select(expression, null, 
                    DataViewRowState.CurrentRows, 
                    new Dictionary<string, object> 
                    {
                        { "@category", category },
                        { "@key", key }
                    });

                return rows.Length > 0 ? rows[0][3].ToString() : string.Empty;
            }
            catch (Exception)
            {
                return string.Empty;
            }
        }

        public static string[] ReadListConfiguration(DataTable dataTable, string category, string key)
        {
            if (dataTable == null || string.IsNullOrEmpty(category) || string.IsNullOrEmpty(key))
                return Array.Empty<string>();

            try
            {
                var expression = "Category = @category AND Key = @key AND Enabled = 1";
                var rows = dataTable.Select(expression, null, 
                    DataViewRowState.CurrentRows,
                    new Dictionary<string, object> 
                    {
                        { "@category", category },
                        { "@key", key }
                    });

                return rows.Select(row => row[3].ToString()).ToArray();
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error("An error occurred in list configuration: {0}", 
                    SecurityElement.Escape(e.Message));
                return Array.Empty<string>();
            }
        }

        public static HttpClient WebAuthenticationWithToken(string connectionString, string app)
        {
            var handler = new HttpClientHandler
            {
                SslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                ServerCertificateCustomValidationCallback = null // Ensure proper certificate validation
            };

            var confClient = new HttpClient(handler);

            try
            {
                string decryptedToken = EncryptionService.ReadEncryptedDataFromDatabase(connectionString, app);
                if (!string.IsNullOrEmpty(decryptedToken))
                {
                    confClient.DefaultRequestHeaders.Authorization = 
                        new AuthenticationHeaderValue("Bearer", decryptedToken);
                    confClient.DefaultRequestHeaders.Accept.Add(
                        new MediaTypeWithQualityHeaderValue("application/json"));
                    confClient.Timeout = TimeSpan.FromMilliseconds(100000000);
                }
            }
            catch (WebException e)
            {
                RallyLoad.logger.Error("An error occurred in HTTP request (web exception): {0}", 
                    SecurityElement.Escape(e.Message));
            }
            catch (Exception e)
            {
                RallyLoad.logger.Error("An error occurred in HTTP request (other exception): {0}", 
                    SecurityElement.Escape(e.Message));
            }
            return confClient;
        }

        public static string WebRequestWithToken(HttpClient confClient, string url)
        {
            if (confClient == null || string.IsNullOrEmpty(url))
                return string.Empty;

            string json = string.Empty;
            bool tryAgain = true;
            int nbRetry = GetSafeRetryCount();

            while (tryAgain && nbRetry > 0)
            {
                try
                {
                    var response = confClient.GetAsync(url).Result;
                    if (response.IsSuccessStatusCode)
                    {
                        json = response.Content.ReadAsStringAsync().Result;
                        if (!string.IsNullOrEmpty(json) && !json.TrimStart().StartsWith("<"))
                        {
                            return HttpUtility.HtmlEncode(json); // Encode output for security
                        }
                    }
                    else
                    {
                        RallyLoad.logger.Error(SecurityElement.Escape(response.ReasonPhrase));
                        nbRetry--;
                    }
                }
                catch (Exception e)
                {
                    RallyLoad.logger.Error("HTTP Response error - retry number: {0} {1}", 
                        nbRetry, SecurityElement.Escape(e.Message));
                    nbRetry--;
                }

                if (nbRetry <= 0)
                {
                    RallyLoad.logger.Error("HTTP Response error - Max retries reached");
                    Environment.Exit(-1);
                }
            }
            return json;
        }

        private static int GetSafeRetryCount()
        {
            int defaultRetry = 3;
            try
            {
                string retryStr = ConfigurationManager.AppSettings["nbRetry"];
                if (int.TryParse(retryStr, out int configuredRetry))
                {
                    return Math.Max(0, Math.Min(configuredRetry, 10)); // Limit between 0 and 10
                }
            }
            catch
            {
                // Fall back to default if configuration is invalid
            }
            return defaultRetry;
        }
    }
}
