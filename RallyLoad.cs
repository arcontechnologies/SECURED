using System;
using System.IO;
using System.Linq;
using System.Xml;
using System.Data;
using System.Data.SqlClient;
using System.Security;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Configuration;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Net.Http;
using System.Collections.Generic;
using NLog;

namespace Rally
{
    class RallyLoad
    {
        public static HttpClient httpclient;
        public static Logger logger = LogManager.GetCurrentClassLogger();
        private static readonly object lockObject = new object();

        private static void ExecuteSecureSqlCommand(string dbServer, string database, 
            string[] listTable, bool isStaging)
        {
            if (string.IsNullOrEmpty(dbServer) || string.IsNullOrEmpty(database) || 
                listTable == null || !listTable.Any())
                return;

            var connectionString = new SqlConnectionStringBuilder
            {
                DataSource = dbServer,
                InitialCatalog = database,
                IntegratedSecurity = true,
                MultipleActiveResultSets = false,
                Encrypt = true
            }.ConnectionString;

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (var cmd = new SqlCommand("dbo.st_truncate_table", connection)
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = 0
                })
                {
                    try
                    {
                        if (isStaging)
                        {
                            foreach (var table in listTable)
                            {
                                cmd.Parameters.Clear();
                                cmd.Parameters.AddWithValue("@Tablename", 
                                    SqlUtility.SanitizeTableName($"[MyDB].[TB_STAGING_{table}]"));
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        logger.Error("Error in SQL statement (truncate tables): {0}", 
                            SecurityElement.Escape(e.Message));
                    }
                }
            }
        }

        private static void BulkInsertDynamic(DataSet dataset, DataTable dataTable, 
            string dbServer, string database, string rootNode)
        {
            if (dataset == null || dataTable == null || string.IsNullOrEmpty(rootNode))
                return;

            string[] listTable = Helpers.ReadListConfiguration(dataTable, "Bulkinsert", rootNode);
            string[] listMappedColumn = Helpers.ReadListConfiguration(dataTable, "mapping", rootNode);

            var connectionString = new SqlConnectionStringBuilder
            {
                DataSource = dbServer,
                InitialCatalog = database,
                IntegratedSecurity = true,
                MultipleActiveResultSets = false,
                Encrypt = true
            }.ConnectionString;

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (var bulkCopy = new SqlBulkCopy(connection)
                {
                    BulkCopyTimeout = 0
                })
                {
                    try
                    {
                        foreach (DataTable table in dataset.Tables)
                        {
                            if (!listTable.Contains(table.TableName))
                                continue;

                            var mappedTable = MapTableColumns(table, dbServer, database, 
                                rootNode, listMappedColumn);
                            
                            bulkCopy.DestinationTableName = 
                                SqlUtility.SanitizeTableName($"[MyDB].[TB_STAGING_{rootNode.ToUpper()}]");
                            
                            foreach (DataColumn column in mappedTable.Columns)
                            {
                                bulkCopy.ColumnMappings.Add(column.ColumnName.Trim(), 
                                    column.ColumnName.Trim());
                            }

                            bulkCopy.WriteToServer(mappedTable);
                        }
                    }
                    catch (Exception e)
                    {
                        logger.Error("Table: {0} -- Error in bulk insert: {1}", 
                            SecurityElement.Escape(rootNode), 
                            SecurityElement.Escape(e.Message));
                    }
                }
            }
        }

        private static DataTable MapTableColumns(DataTable sourceTable, string dbServer, 
            string database, string rootNode, string[] mappedColumns)
        {
            var resultTable = sourceTable.Copy();
            var sqlTable = Helpers.GetSQLTable(dbServer, database, 
                $"TB_STAGING_{rootNode.ToUpper()}");
            
            return Helpers.CompareRows(sqlTable, 
                Helpers.GetAGTable(sourceTable), 
                resultTable, 
                mappedColumns);
        }

        private static void UpdateDataTable()
        {
            var dbServer = ConfigurationManager.AppSettings["dbserver"];
            var database = ConfigurationManager.AppSettings["database"];
            var configTable = ConfigurationManager.AppSettings["configtable"];

            if (string.IsNullOrEmpty(dbServer) || string.IsNullOrEmpty(database) || 
                string.IsNullOrEmpty(configTable))
                return;

            var configTableToLoad = Helpers.LoadConfiguration(dbServer, database, configTable);
            var connectionString = new SqlConnectionStringBuilder
            {
                DataSource = dbServer,
                InitialCatalog = database,
                IntegratedSecurity = true,
                MultipleActiveResultSets = false,
                Encrypt = true
            }.ConnectionString;

            using (var connection = new SqlConnection(connectionString))
            {
                var dataTable = new DataTable();
                using (var command = new SqlCommand(
                    "SELECT * FROM [MyDB].[TB_STAGING_FEATURE_RevisionHistory]", 
                    connection))
                {
                    connection.Open();
                    using (var adapter = new SqlDataAdapter(command))
                    {
                        adapter.Fill(dataTable);
                    }
                }

                var counter = 0;
                foreach (DataRow row in dataTable.Rows)
                {
                    var url = $"{row["_ref"]}/Revisions?query=(RevisionNumber = \"0\")";
                    var json = Helpers.WebRequestWithToken(httpclient, url);
                    
                    if (string.IsNullOrEmpty(json)) continue;

                    var rss = JObject.Parse(json);
                    var featureCreatorId = (string)rss.SelectToken(
                        "QueryResult.Results[0].User._refObjectUUID");
                    
                    if (!string.IsNullOrEmpty(featureCreatorId))
                    {
                        row.BeginEdit();
                        row["_type"] = SecurityElement.Escape(featureCreatorId);
                        row.EndEdit();
                        counter++;
                    }
                }

                using (var command = new SqlCommand("dbo.st_truncate_table", connection))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.AddWithValue("@Tablename", 
                        "[MyDB].[TB_STAGING_FEATURE_RevisionHistory]");
                    command.ExecuteNonQuery();

                    using (var bulkCopy = new SqlBulkCopy(connection))
                    {
                        bulkCopy.DestinationTableName = 
                            "[MyDB].[TB_STAGING_FEATURE_RevisionHistory]";
                        bulkCopy.WriteToServer(dataTable);
                    }
                }
            }

            logger.Info("# {0} Feature IDs were created", counter);
        }

        private static void GetMilestones(string table, string portfolioItemType)
        {
            if (string.IsNullOrEmpty(table) || string.IsNullOrEmpty(portfolioItemType))
                return;

            var dbServer = ConfigurationManager.AppSettings["dbserver"];
            var database = ConfigurationManager.AppSettings["database"];
            var configTable = ConfigurationManager.AppSettings["configtable"];

            if (string.IsNullOrEmpty(dbServer) || string.IsNullOrEmpty(database) || 
                string.IsNullOrEmpty(configTable))
                return;

            var configTableToLoad = Helpers.LoadConfiguration(dbServer, database, configTable);
            var connectionString = new SqlConnectionStringBuilder
            {
                DataSource = dbServer,
                InitialCatalog = database,
                IntegratedSecurity = true,
                MultipleActiveResultSets = false,
                Encrypt = true
            }.ConnectionString;

            using (var connection = new SqlConnection(connectionString))
            {
                var milestonesRef = new DataTable();
                using (var command = new SqlCommand(
                    @"SELECT M._ref as url, P.ObjectUUID as PortfolioItemID 
                    FROM [MyDB].[TB_STAGING_" + table.ToUpper() + @"_Milestones] M 
                    INNER JOIN [MyDB].[TB_STAGING_" + table.ToUpper() + @"] P 
                    ON (P.Results_Id = M.Results_Id) 
                    WHERE count > 0", connection))
                {
                    connection.Open();
                    using (var adapter = new SqlDataAdapter(command))
                    {
                        adapter.Fill(milestonesRef);
                    }
                }

                var dataset = new DataSet();
                var firstTime = true;

                foreach (DataRow row in milestonesRef.Rows)
                {
                    var url = row["url"].ToString();
                    var json = Helpers.WebRequestWithToken(httpclient, url);

                    if (string.IsNullOrEmpty(json) || json.TrimStart().StartsWith("<"))
                        continue;

                    try
                    {
                        var doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json);
                        using (var reader = new XmlTextReader(
                            new StringReader(doc.OuterXml)))
                        {
                            dataset.ReadXml(reader);
                        }

                        if (firstTime)
                        {
                            AddPortfolioColumns(dataset, row["PortfolioItemID"].ToString(), 
                                portfolioItemType);
                            firstTime = false;
                        }
                        else
                        {
                            UpdatePortfolioData(dataset, row["PortfolioItemID"].ToString(), 
                                portfolioItemType);
                        }

                        BulkInsertDynamic(dataset, configTableToLoad, dbServer, 
                            database, "milestones");
                        dataset.Clear();
                    }
                    catch (Exception e)
                    {
                        logger.Error("Error processing milestones: {0}", 
                            SecurityElement.Escape(e.Message));
                    }
                }
            }

            logger.Info("Milestones inserted into SQL");
        }

        private static void AddPortfolioColumns(DataSet dataset, string portfolioItemId, 
            string portfolioItemType)
        {
            if (!dataset.Tables.Contains("Results"))
                return;

            dataset.Tables["Results"].Columns.Add(new DataColumn
            {
                ColumnName = "PortfolioItemID",
                DataType = typeof(string)
            });

            dataset.Tables["Results"].Columns.Add(new DataColumn
            {
                ColumnName = "PortfolioItemType",
                DataType = typeof(string)
            });

            UpdatePortfolioData(dataset, portfolioItemId, portfolioItemType);
        }

        private static void UpdatePortfolioData(DataSet dataset, string portfolioItemId, 
            string portfolioItemType)
        {
            if (!dataset.Tables.Contains("Results"))
                return;

            foreach (DataRow row in dataset.Tables["Results"].Rows)
            {
                row["PortfolioItemID"] = SecurityElement.Escape(portfolioItemId);
                row["PortfolioItemType"] = SecurityElement.Escape(portfolioItemType);
            }
        }

        private static class SqlUtility
        {
            public static string SanitizeTableName(string tableName)
            {
                if (string.IsNullOrEmpty(tableName))
                    return string.Empty;

                // Remove any dangerous characters and limit length
                var sanitized = Regex.Replace(tableName, @"[^a-zA-Z0-9\[\]_.]", "");
                return sanitized.Length > 128 ? sanitized.Substring(0, 128) : sanitized;
            }
        }

        public static void Main(string[] args)
        {
            try
            {
                var environment = ConfigurationManager.AppSettings["ENV"];
                var dbServer = ConfigurationManager.AppSettings["dbserver"];
                var database = ConfigurationManager.AppSettings["database"];
                var configTable = ConfigurationManager.AppSettings["configtable"];

                if (string.IsNullOrEmpty(dbServer) || string.IsNullOrEmpty(database) || 
                    string.IsNullOrEmpty(configTable))
                {
                    logger.Error("Missing required configuration settings");
                    return;
                }

                var configTableToLoad = Helpers.LoadConfiguration(dbServer, database, configTable);
                var connectionString = new SqlConnectionStringBuilder
                {
                    DataSource = dbServer,
                    InitialCatalog = database,
                    IntegratedSecurity = true,
                    MultipleActiveResultSets = false,
                    Encrypt = true
                }.ConnectionString;

                httpclient = Helpers.WebAuthenticationWithToken(connectionString, "myApp");

                var listTableToTruncate = Helpers.ReadListConfiguration(
                    configTableToLoad, "dbstatement", "truncate");
                var listTableToLoad = Helpers.ReadListConfiguration(
                    configTableToLoad, "load", "load");

                ExecuteSecureSqlCommand(dbServer, database, listTableToTruncate, true);

                var tasks = new List<Task>();
                foreach (var table in listTableToLoad)
                {
                    var task = Task.Factory.StartNew(() =>
                    {
                        using (var sqlTargetSet = new DataSet())
                        {
                            LoadDataFromAgile(sqlTargetSet, dbServer, database, 
                                configTableToLoad, table);
                        }
                    }, TaskCreationOptions.LongRunning);
                    
                    tasks.Add(task);
                }

                Task.WaitAll(tasks.ToArray());

                LoadFeatureException(dbServer, database, "feature", configTableToLoad);
                logger.Info("Load into SQL Server Staging has been completed");
            }
            catch (Exception e)
            {
                logger.Error("Critical error in main execution: {0}", 
                    SecurityElement.Escape(e.Message));
            }
            finally
            {
                httpclient?.Dispose();
            }
        }
    }
}
