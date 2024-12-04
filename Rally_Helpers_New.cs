using System;
using System.Net;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Net.Http;
using System.Net.Http.Headers;
using DataEncryption;
using System.Collections.Generic;
using System.Security.Principal;
using System.Security;
using System.Text.RegularExpressions;

namespace Rally
{
    public class Helpers
    {
        private const int DEFAULT_COMMAND_TIMEOUT = 0;
        private static readonly HashSet<string> ValidColumnTypes = new HashSet<string> 
        {
            "String", "Int32", "DateTime", "Decimal", "Boolean"
        };

        public static class SecurityValidation
        {
            private static readonly HashSet<string> ValidTables = new HashSet<string>
            {
                "FEATURE", "USERSTORY", "ITERATION", "RISK", "OPUS", "INITIATIVE",
                "MILESTONES", "REVISIONHISTORY", "REVISION", "EXPERTISE", "LINKS"
            };

            public static string GetSanitizedTableName(string tableName)
            {
                string upperTableName = tableName?.ToUpper() ?? 
                    throw new ArgumentNullException(nameof(tableName));

                if (!ValidTables.Contains(upperTableName))
                {
                    throw new SecurityException($"Invalid table name: {upperTableName}");
                }

                return $"[MyDB].[TB_STAGING_{upperTableName}]";
            }

            public static string GetSanitizedColumnName(string columnName)
            {
                if (string.IsNullOrWhiteSpace(columnName))
                {
                    throw new ArgumentNullException(nameof(columnName));
                }

                // Only allow alphanumeric and underscore
                if (!Regex.IsMatch(columnName, @"^[a-zA-Z0-9_]+$"))
                {
                    throw new SecurityException($"Invalid column name format: {columnName}");
                }

                return $"[{columnName}]";
            }

            public static string GetUserContext()
            {
                return WindowsIdentity.GetCurrent().Name;
            }
        }

        public static DataTable GetSQLTable(string dbserver, string database, string tableName)
        {
            DataTable sqlTable = new DataTable();
            
            using (var connection = new SqlConnection(GetConnectionString(dbserver, database)))
            {
                try
                {
                    var query = new SqlCommand
                    {
                        Connection = connection,
                        CommandType = CommandType.Text,
                        CommandTimeout = DEFAULT_COMMAND_TIMEOUT,
                        CommandText = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TableName"
                    };
                    query.Parameters.AddWithValue("@TableName", tableName);

                    connection.Open();
                    using (var adapter = new SqlDataAdapter(query))
                    {
                        adapter.Fill(sqlTable);
                    }
                }
                catch (Exception e)
                {
                    string sanitizedMessage = SanitizeErrorMessage(e.Message);
                    RallyLoad.logger.Error(sanitizedMessage, "An error occurred in GetSQLTable");
                    throw;
                }
            }
            return sqlTable;
        }

        public static DataTable GetAGTable(DataTable datatable)
        {
            DataTable columnTable = new DataTable();
            try
            {
                string[] columnNames = datatable.Columns.Cast<DataColumn>()
                    .Select(dc => dc.ColumnName).ToArray();

                columnTable.Columns.Add(new DataColumn
                {
                    ColumnName = "COLUMN_NAME",
                    DataType = Type.GetType("System.String")
                });

                foreach (string columnName in columnNames)
                {
                    DataRow newRow = columnTable.NewRow();
                    newRow["COLUMN_NAME"] = SecurityValidation.GetSanitizedColumnName(columnName);
                    columnTable.Rows.Add(newRow);
                }
            }
            catch (Exception e)
            {
                string sanitizedMessage = SanitizeErrorMessage(e.Message);
                RallyLoad.logger.Error(sanitizedMessage, "An error occurred in GetAGTable");
                throw;
            }
            return columnTable;
        }

        public static Tuple<bool, string> GetMappedColumn(string[] mappedConfiguration, string agColumnName)
        {
            if (mappedConfiguration == null || string.IsNullOrEmpty(agColumnName))
            {
                throw new ArgumentNullException(
                    mappedConfiguration == null ? nameof(mappedConfiguration) : nameof(agColumnName));
            }

            foreach (string mapping in mappedConfiguration)
            {
                int separatorIndex = mapping.LastIndexOf(':');
                if (separatorIndex == -1) continue;

                string mappedAGColumn = mapping.Substring(separatorIndex + 1);
                if (mappedAGColumn.Equals(agColumnName, StringComparison.OrdinalIgnoreCase))
                {
                    string mappedSQLColumn = mapping.Substring(0, mapping.IndexOf(':'));
                    return Tuple.Create(true, SecurityValidation.GetSanitizedColumnName(mappedSQLColumn));
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
                string sanitizedMessage = SanitizeErrorMessage(e.Message);
                RallyLoad.logger.Error(sanitizedMessage, "An error occurred in CompareRows");
                throw;
            }
            return resultTable;
        }

        public static string GetConnectionString(string dbserver, string database)
        {
            try
            {
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
                {
                    DataSource = dbserver,
                    InitialCatalog = database,
                    IntegratedSecurity = true,
                    TrustServerCertificate = true,
                    MultipleActiveResultSets = true,
                    ApplicationName = "RallyLoad"
                };
                
                return builder.ConnectionString;
            }
            catch (Exception e)
            {
                string sanitizedMessage = SanitizeErrorMessage(e.Message);
                RallyLoad.logger.Error(sanitizedMessage, "An error occurred creating connection string");
                throw;
            }
        }

        public static DataTable LoadConfiguration(string dbserver, string database, string configTable)
        {
            DataTable dataTable = new DataTable();
            string sanitizedTableName = SecurityValidation.GetSanitizedTableName(configTable);

            using (var connection = new SqlConnection(GetConnectionString(dbserver, database)))
            {
                try
                {
                    var query = new SqlCommand
                    {
                        Connection = connection,
                        CommandType = CommandType.Text,
                        CommandTimeout = DEFAULT_COMMAND_TIMEOUT,
                        CommandText = $"SELECT * FROM {sanitizedTableName}"
                    };

                    connection.Open();
                    using (var adapter = new SqlDataAdapter(query))
                    {
                        adapter.Fill(dataTable);
                    }
                }
                catch (Exception e)
                {
                    string sanitizedMessage = SanitizeErrorMessage(e.Message);
                    RallyLoad.logger.Error(sanitizedMessage, "An error occurred loading configuration");
                    throw;
                }
            }
            return dataTable;
        }

        public static string ReadConfiguration(DataTable dataTable, string category, string key)
        {
            if (dataTable == null)
                throw new ArgumentNullException(nameof(dataTable));

            using (var command = new SqlCommand())
            {
                command.Parameters.AddWithValue("@Category", category);
                command.Parameters.AddWithValue("@Key", key);
                
                string expression = "Category = @Category AND Key = @Key";
                DataRow[] rows = dataTable.Select(expression);

                if (rows.Length == 0)
                    throw new InvalidOperationException($"Configuration not found for category: {category}, key: {key}");

                return rows[0][3].ToString();
            }
        }

        public static string[] ReadListConfiguration(DataTable dataTable, string category, string key)
        {
            if (dataTable == null)
                throw new ArgumentNullException(nameof(dataTable));

            using (var command = new SqlCommand())
            {
                command.Parameters.AddWithValue("@Category", category);
                command.Parameters.AddWithValue("@Key", key);
                
                DataRow[] rows = dataTable.Select(
                    "Category = @Category AND Key = @Key AND Enabled = 1", 
                    command.Parameters);

                string[] result = new string[rows.Length];
                
                for (int i = 0; i < rows.Length; i++)
                {
                    result[i] = rows[i][3].ToString();
                }

                return result;
            }
        }

        public static HttpClient WebAuthenticationWithToken(string connectionString, string app)
        {
            var handler = new HttpClientHandler
            {
                SslProtocols = System.Security.Authentication.SslProtocols.Tls12
            };

            var client = new HttpClient(handler);

            try
            {
                string decryptedToken = EncryptionService.ReadEncryptedDataFromDatabase(
                    connectionString, app);
                
                client.DefaultRequestHeaders.Add("Authorization", $"Bearer {decryptedToken}");
                client.DefaultRequestHeaders.Accept.Add(
                    new MediaTypeWithQualityHeaderValue("application/json"));
                client.Timeout = TimeSpan.FromMilliseconds(100000000);
            }
            catch (Exception e)
            {
                string sanitizedMessage = SanitizeErrorMessage(e.Message);
                RallyLoad.logger.Error(sanitizedMessage, "An error occurred in WebAuthentication");
                throw;
            }

            return client;
        }

        public static string WebRequestWithToken(HttpClient client, string url)
        {
            if (client == null)
                throw new ArgumentNullException(nameof(client));
            if (string.IsNullOrEmpty(url))
                throw new ArgumentNullException(nameof(url));

            string json = string.Empty;
            int nbRetry = Convert.ToInt32(ConfigurationManager.AppSettings["nbRetry"]);
            int currentRetry = 0;

            while (currentRetry < nbRetry)
            {
                try
                {
                    HttpResponseMessage response = client.GetAsync(url).Result;

                    if (response.IsSuccessStatusCode)
                    {
                        json = response.Content.ReadAsStringAsync().Result;
                        if (!json.TrimStart().StartsWith("<"))
                        {
                            return json.Replace("\"", @"""");
                        }
                    }
                    else
                    {
                        string errorMessage = $"HTTP {(int)response.StatusCode}: {response.ReasonPhrase}";
                        RallyLoad.logger.Error(errorMessage);
                    }
                }
                catch (Exception e)
                {
                    string sanitizedMessage = SanitizeErrorMessage(e.Message);
                    RallyLoad.logger.Error(sanitizedMessage, 
                        $"Retry {currentRetry + 1}/{nbRetry}: Web request failed");
                }

                currentRetry++;
                if (currentRetry == nbRetry)
                {
                    throw new ApplicationException(
                        "Maximum number of retry attempts reached for web request");
                }
            }

            return json;
        }

        private static string SanitizeErrorMessage(string message)
        {
            if (string.IsNullOrEmpty(message))
                return string.Empty;

            return message.Replace("\r", "").Replace("\n", "")
                .Replace("\t", " ").Replace(";", "");
        }
    }
}
