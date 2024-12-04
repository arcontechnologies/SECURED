using System;
using System.IO;
using System.Linq;
using System.Xml;
using System.Data;
using System.Data.SqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Configuration;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Net.Http;
using NLog;
using System.Collections.Generic;
using System.Security;

namespace Rally
{
    public class RallyLoad
    {
        public static readonly Logger logger = LogManager.GetCurrentClassLogger();
        private static HttpClient httpclient;
        private const int DEFAULT_COMMAND_TIMEOUT = 0;
        private const int DEFAULT_PAGE_SIZE = 2000;
        private static readonly HashSet<string> ValidTables = new HashSet<string>
        {
            "FEATURE", "USERSTORY", "ITERATION", "RISK", "OPUS", "INITIATIVE", "MILESTONES",
            "REVISIONHISTORY", "REVISION", "EXPERTISE", "LINKS", "PARENT"
        };

        private class SecureConnectionManager : IDisposable
        {
            private SqlConnection _connection;
            private readonly string _connectionString;

            public SecureConnectionManager(string dbServer, string database)
            {
                _connectionString = Helpers.GetConnectionString(dbServer, database);
                _connection = new SqlConnection(_connectionString);
            }

            public SqlConnection GetConnection()
            {
                if (_connection.State != ConnectionState.Open)
                {
                    _connection.Open();
                }
                return _connection;
            }

            public void Dispose()
            {
                if (_connection != null)
                {
                    if (_connection.State == ConnectionState.Open)
                    {
                        _connection.Close();
                    }
                    _connection.Dispose();
                    _connection = null;
                }
            }
        }

        private static void ValidateTableName(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new SecurityException("Table name cannot be null or empty");
            }

            string normalizedTableName = tableName.ToUpperInvariant();
            if (!ValidTables.Contains(normalizedTableName))
            {
                throw new SecurityException($"Invalid table name: {tableName}");
            }
        }

        private static async Task SQLstatement(string dbserver, string database, string[] listtable, bool is_stagging)
        {
            using (var connManager = new SecureConnectionManager(dbserver, database))
            {
                var connection = connManager.GetConnection();

                if (is_stagging)
                {
                    foreach (var table in listtable)
                    {
                        ValidateTableName(table);
                        try
                        {
                            using (SqlCommand cmd = new SqlCommand("dbo.st_truncate_table", connection))
                            {
                                cmd.CommandType = CommandType.StoredProcedure;
                                cmd.CommandTimeout = DEFAULT_COMMAND_TIMEOUT;
                                cmd.Parameters.AddWithValue("@Tablename",
                                    Helpers.SecurityValidation.GetSanitizedTableName(table));
                                await cmd.ExecuteNonQueryAsync();
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.Error(ex, $"Error truncating table {table}");
                            throw;
                        }
                    }
                }
            }
        }

        private static async Task BulkInsertDynamic(DataSet dataset, DataTable configTable,
            string dbServer, string database, string rootNode)
        {
            ValidateTableName(rootNode);

            var listTable = Helpers.ReadListConfiguration(configTable, "Bulkinsert", rootNode);
            var listMappedColumn = Helpers.ReadListConfiguration(configTable, "mapping", rootNode);

            using (var connManager = new SecureConnectionManager(dbServer, database))
            {
                var connection = connManager.GetConnection();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.BulkCopyTimeout = DEFAULT_COMMAND_TIMEOUT;

                    for (int count = 0; count < dataset.Tables.Count; count++)
                    {
                        if (!listTable.Contains(dataset.Tables[count].TableName))
                            continue;

                        var inputDataTableMapping = dataset.Tables[count];
                        var resultDataTableMapping = await MapDataTableColumns(
                            dbServer, database, rootNode, inputDataTableMapping,
                            listMappedColumn, count == 0);

                        bulkCopy.DestinationTableName =
                            Helpers.SecurityValidation.GetSanitizedTableName(rootNode);
                        await PerformBulkInsert(bulkCopy, resultDataTableMapping);
                    }
                }
            }
        }

        private static async Task LoadDataFromAgile(DataSet SQLTargetSet, string dbserver,
            string database, DataTable ConfigTable, string table)
        {
            int BlockSize = Convert.ToInt32(ConfigurationManager.AppSettings["BlockSize"]);

            try
            {
                ValidateTableName(table);
                logger.Info($"Task load for {table} table has started");

                await LoadDataset(SQLTargetSet, ConfigTable, table, $"{table}_count", BlockSize);

                if (table.Equals("userstory", StringComparison.OrdinalIgnoreCase))
                {
                    logger.Info("Managing exceptions for UserStory table");
                    using (var ExceptionDataset = new DataSet())
                    {
                        await LoadDatasetExceptionUserstory(ExceptionDataset, ConfigTable, table, $"{table}_count");
                    }
                }
                else if (table.Equals("risk", StringComparison.OrdinalIgnoreCase))
                {
                    logger.Info("Processing risk-related items");
                    await MakeLinksForAffectedItemsForRisk(dbserver, database);
                }

                logger.Info($"Task load for {table} table completed");
            }
            catch (Exception ex)
            {
                logger.Error(ex, $"Error processing table {table}");
                throw;
            }
            finally
            {
                SQLTargetSet?.Dispose();
            }
        }

        private static async Task LoadDataset(DataSet dataset, DataTable ConfigTable,
            string table, string tableCount, int pageSize)
        {
            ValidateTableName(table);

            int addDays = Convert.ToInt32(ConfigurationManager.AppSettings["AddDays"]);
            string lastUpdateDate = DateTime.Today.AddDays(-addDays).ToString("yyyy-MM-dd");

            var urlBase = Helpers.ReadConfiguration(ConfigTable, "url", table);
            var urlCount = Helpers.ReadConfiguration(ConfigTable, "url", tableCount);

            string query = BuildQueryString(table, lastUpdateDate);
            urlCount = $"{urlCount}&fetch=ObjectUUID&start=1&pagesize=1{query}";

            int totalResults = GetTotalResultCount(urlCount);
            int iterations = (int)Math.Ceiling((double)totalResults / pageSize);

            for (int i = 0; i < iterations; i++)
            {
                int start = (i * pageSize) + 1;
                string url = $"{urlBase}&fetch=true&start={start}&pagesize={pageSize}{query}";

                ProcessDatasetBatch(url, dataset, table);
                await BulkInsertDynamic(dataset, ConfigTable,
                    ConfigurationManager.AppSettings["dbserver"],
                    ConfigurationManager.AppSettings["database"],
                    table);

                dataset.Clear();
            }
        }

        private static async Task LoadDatasetExceptionUserstory(DataSet dataset,
            DataTable ConfigTable, string table, string tableCount)
        {
            int addDays = Convert.ToInt32(ConfigurationManager.AppSettings["AddDays"]);
            string lastUpdateDate = DateTime.Today.AddDays(-addDays).ToString("yyyy-MM-dd");

            string url = BuildUserStoryExceptionUrl(lastUpdateDate);
            await ProcessUserStoryExceptions(url, dataset, ConfigTable);
        }

        private static async Task MakeLinksForAffectedItemsForRisk(string dbserver, string database)
        {
            using (var connManager = new SecureConnectionManager(dbserver, database))
            {
                var connection = connManager.GetConnection();

                using (var cmd = new SqlCommand())
                {
                    cmd.Connection = connection;
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "SELECT * FROM TB_STAGING_RISK_WorkItemsAffected WHERE [Count] > 0 ORDER BY [Results_Id]";

                    using (var adapter = new SqlDataAdapter(cmd))
                    {
                        var refLinks = new DataTable();
                        adapter.Fill(refLinks);
                        await ProcessRiskLinks(refLinks, connection);
                    }
                }
            }
        }

        private static async Task LoadFeatureException(string dbserver, string database,
            string table, DataTable ConfigTable)
        {
            ValidateTableName(table);

            using (var connManager = new SecureConnectionManager(dbserver, database))
            {
                var connection = connManager.GetConnection();

                int addDays = Convert.ToInt32(ConfigurationManager.AppSettings["AddDays"]);
                string lastUpdateDate = DateTime.Today.AddDays(-addDays).ToString("yyyy-MM-dd");

                string urlBase = Helpers.ReadConfiguration(ConfigTable, "url", table);
                string query = $"&query=((LastUpdateDate >= {lastUpdateDate}) AND (c_RequiredDeliveryDate != null))";

                await ProcessFeatureExceptions(urlBase, query, connection);
            }
        }

        public static async Task Main(string[] args)
        {
            try
            {
                var environment = ConfigurationManager.AppSettings["ENV"];
                var dbserver = ConfigurationManager.AppSettings["dbserver"];
                var database = ConfigurationManager.AppSettings["database"];
                var configtable = ConfigurationManager.AppSettings["configtable"];

                if (string.IsNullOrEmpty(dbserver) || string.IsNullOrEmpty(database))
                {
                    throw new ConfigurationErrorsException("Database configuration is missing or invalid");
                }

                var configTableToLoad = await Task.Run(() =>
                    Helpers.LoadConfiguration(dbserver, database, configtable));

                var tablesToTruncate = Helpers.ReadListConfiguration(
                    configTableToLoad, "dbstatement", "truncate");
                var tablesToLoad = Helpers.ReadListConfiguration(
                    configTableToLoad, "load", "load");

                using (var connManager = new SecureConnectionManager(dbserver, database))
                {
                    httpclient = Helpers.WebAuthenticationWithToken(
                        connManager.GetConnection().ConnectionString, "myApp");

                    await SQLstatement(dbserver, database, tablesToTruncate, true);

                    var tasks = tablesToLoad.Select(table =>
                        Task.Run(() => LoadDataFromAgile(
                            new DataSet(), dbserver, database, configTableToLoad, table))).ToArray();

                    await Task.WhenAll(tasks);

                    await LoadFeatureException(dbserver, database, "feature", configTableToLoad);
                    logger.Info("Load into SQL Server Staging has been completed");
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex, "Fatal error in main execution: {0}", ex.Message);
                throw;
            }
            finally
            {
                httpclient?.Dispose();
            }
        }

        private static int GetTotalResultCount(string url)
        {
            string response = Helpers.WebRequestWithToken(httpclient, url);
            JObject resultObject = JObject.Parse(response);
            return (int)resultObject.SelectToken("QueryResult.TotalResultCount");
        }

        private static void ProcessDatasetBatch(string url, DataSet dataset, string table)
        {
            string response = Helpers.WebRequestWithToken(httpclient, url);
            if (!response.TrimStart().StartsWith("<"))
            {
                XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(response);
                using (var reader = new StringReader(doc.OuterXml))
                using (var xmlReader = XmlReader.Create(reader))
                {
                    dataset.ReadXml(xmlReader);
                }
            }
        }

        private static string BuildQueryString(string table, string lastUpdateDate)
        {
            switch (table.ToLowerInvariant())
            {
                case "userstory":
                case "feature":
                    return $"&query=(LastUpdateDate >= {lastUpdateDate})";
                case "iteration":
                    return $"&query=(CreationDate >= {lastUpdateDate})";
                default:
                    return string.Empty;
            }
        }

        private static async Task ProcessRiskLinks(DataTable refLinks, SqlConnection connection)
        {
            var targetTable = new DataTable();
            targetTable.Columns.Add("ObjectIDRisk", typeof(string));
            targetTable.Columns.Add("ObjectIDAffectedItem", typeof(string));
            targetTable.Columns.Add("Type", typeof(string));

            foreach (DataRow row in refLinks.Rows)
            {
                string url = $"{row["_ref"]}/Revisions?fetch=_refObjectUUID&start=1&pagesize={DEFAULT_PAGE_SIZE}";
                string objectIdRisk = Regex.Match(url, @"[^\d](\d{11})[^\d]").Value.Replace("/", "");

                string response = Helpers.WebRequestWithToken(httpclient, url);
                ProcessRiskLinkResponse(response, objectIdRisk, targetTable);
            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = Helpers.SecurityValidation.GetSanitizedTableName("LINKS");
                await bulkCopy.WriteToServerAsync(targetTable);
            }
        }

        private static void ProcessRiskLinkResponse(string response, string objectIdRisk, DataTable targetTable)
        {
            if (!response.TrimStart().StartsWith("<"))
            {
                XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(response);
                using (var reader = new StringReader(doc.OuterXml))
                using (var xmlReader = XmlReader.Create(reader))
                {
                    var tempDataSet = new DataSet();
                    tempDataSet.ReadXml(xmlReader);

                    if (tempDataSet.Tables["Results"] != null)
                    {
                        foreach (DataRow row in tempDataSet.Tables["Results"].Rows)
                        {
                            var newRow = targetTable.NewRow();
                            newRow["ObjectIDRisk"] = objectIdRisk;
                            newRow["ObjectIDAffectedItem"] = row["_refObjectUUID"];
                            newRow["Type"] = row["_type"];
                            targetTable.Rows.Add(newRow);
                        }
                    }
                }
            }
        }

        private static async Task ProcessFeatureExceptions(string urlBase, string query, SqlConnection connection)
        {
            string url = urlBase + query;
            string response = Helpers.WebRequestWithToken(httpclient, url);
            JObject resultObject = JObject.Parse(response);
            int totalResults = (int)resultObject.SelectToken("QueryResult.TotalResultCount");
            double iterations = Math.Ceiling((double)totalResults / DEFAULT_PAGE_SIZE);

            var dtException = new DataTable();
            dtException.Columns.Add("_rallyAPIMajor", typeof(string));
            dtException.Columns.Add("_rallyAPIMinor", typeof(string));

            for (int i = 1; i <= iterations; i++)
            {
                int start = ((i - 1) * DEFAULT_PAGE_SIZE) + 1;
                string paginatedUrl = $"{urlBase}&fetch=FormattedID,c_RequiredDeliveryDate&start={start}&pagesize={DEFAULT_PAGE_SIZE}{query}";

                string batchResponse = Helpers.WebRequestWithToken(httpclient, paginatedUrl);
                JObject batchResults = JObject.Parse(batchResponse);

                for (int j = 0; j < Math.Min(DEFAULT_PAGE_SIZE, totalResults - ((i - 1) * DEFAULT_PAGE_SIZE)); j++)
                {
                    string formattedId = (string)batchResults.SelectToken($"QueryResult.Results[{j}].FormattedID");
                    string requiredDeliveryDate = (string)batchResults.SelectToken($"QueryResult.Results[{j}].c_RequiredDeliveryDate");

                    if (!string.IsNullOrEmpty(formattedId))
                    {
                        var newRow = dtException.NewRow();
                        newRow["_rallyAPIMajor"] = formattedId;
                        newRow["_rallyAPIMinor"] = requiredDeliveryDate;
                        dtException.Rows.Add(newRow);
                    }
                }
            }

            // Update database with exception data
            using (SqlCommand cmd = new SqlCommand("dbo.st_truncate_table", connection))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandTimeout = DEFAULT_COMMAND_TIMEOUT;
                cmd.Parameters.AddWithValue("@Tablename",
                    Helpers.SecurityValidation.GetSanitizedTableName("Expertise"));
                await cmd.ExecuteNonQueryAsync();
            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName =
                    Helpers.SecurityValidation.GetSanitizedTableName("FEATURE");
                await bulkCopy.WriteToServerAsync(dtException);
            }

            using (SqlCommand cmd = new SqlCommand())
            {
                cmd.Connection = connection;
                cmd.CommandText = Helpers.SecurityValidation.GetSanitizedTableName("query");
                cmd.CommandType = CommandType.Text;
                cmd.CommandTimeout = DEFAULT_COMMAND_TIMEOUT;
                await cmd.ExecuteNonQueryAsync();
            }
        }

        private static string BuildUserStoryExceptionUrl(string lastUpdateDate)
        {
            return $"https://eu1.rallydev.com/slm/webservice/v2.0/hierarchicalrequirement?" +
                   $"workspace=https://eu1.rallydev.com/slm/webservice/v2.0/workspace/99999999999" +
                   $"&query=((Parent != null) AND (LastUpdateDate >= {lastUpdateDate}))" +
                   $"&fetch=ObjectUUID,Parent";
        }

        private static async Task ProcessUserStoryExceptions(string url, DataSet dataset, DataTable ConfigTable)
        {
            string response = Helpers.WebRequestWithToken(httpclient, url);
            if (!response.TrimStart().StartsWith("<"))
            {
                XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(response);
                using (var reader = new StringReader(doc.OuterXml))
                using (var xmlReader = XmlReader.Create(reader))
                {
                    dataset.ReadXml(xmlReader);
                }

                if (dataset.Tables.Contains("Parent"))
                {
                    await ProcessParentTable(dataset, ConfigurationManager.AppSettings["dbserver"],
                        ConfigurationManager.AppSettings["database"]);
                }
            }
        }

        private static async Task ProcessParentTable(DataSet dataset, string dbServer, string database)
        {
            string[] destColumns = { "_rallyAPIMajor", "_rallyAPIMinor", "_ref",
                "_refObjectUUID", "_refObjectName", "_type", "Results_Id" };

            if (!dataset.Tables.Contains("Parent") || !dataset.Tables.Contains("Results"))
                return;

            var parentTable = dataset.Tables["Parent"];
            var resultsTable = dataset.Tables["Results"];

            for (int i = 0; i < parentTable.Rows.Count; i++)
            {
                if (i < resultsTable.Rows.Count)
                {
                    parentTable.Rows[i]["_ref"] = resultsTable.Rows[i]["ObjectUUID"];
                }
            }

            using (var connManager = new SecureConnectionManager(dbServer, database))
            {
                var connection = connManager.GetConnection();
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName =
                        Helpers.SecurityValidation.GetSanitizedTableName("USERSTORY");

                    foreach (string column in destColumns)
                    {
                        if (parentTable.Columns.Contains(column))
                        {
                            bulkCopy.ColumnMappings.Add(column, column);
                        }
                    }

                    await bulkCopy.WriteToServerAsync(parentTable);
                }
            }
        }

        private static async Task<DataTable> MapDataTableColumns(string dbServer, string database,
            string rootNode, DataTable inputTable, string[] mappedColumns, bool isFirstTable)
        {
            var sqlTable = isFirstTable ?
                await GetSQLTableColumns(dbServer, database, $"TB_STAGING_{rootNode}") :
                await GetSQLTableColumns(dbServer, database, rootNode);

            return Helpers.CompareRows(sqlTable,
                Helpers.GetAGTable(inputTable), inputTable, mappedColumns);
        }

        private static async Task<DataTable> GetSQLTableColumns(string dbServer, string database,
            string tableName)
        {
            ValidateTableName(tableName);
            return await Task.Run(() =>
                Helpers.GetSQLTable(dbServer, database, tableName));
        }

        private static async Task PerformBulkInsert(SqlBulkCopy bulkCopy, DataTable dataTable)
        {
            try
            {
                bulkCopy.ColumnMappings.Clear();
                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName.Trim(), column.ColumnName.Trim());
                }
                await bulkCopy.WriteToServerAsync(dataTable);
            }
            catch (Exception ex)
            {
                logger.Error(ex, "Error during bulk insert: {0}", ex.Message);
                throw;
            }
        }
    }
}
