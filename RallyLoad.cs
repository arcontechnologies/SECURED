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
using System.Security;

namespace Rally
{
    class RallyLoad
    {
        public static HttpClient httpclient;
        public static Logger logger = LogManager.GetCurrentClassLogger();
        private const int CommandTimeout = 300;

        static void SQLstatement(string dbserver, string database, string[] listtable, bool is_stagging)
        {
            using (SqlConnection connection = new SqlConnection(ConfigurationManager.ConnectionStrings["RallyDB"].ConnectionString))
            {
                connection.Open();
                using (SqlCommand cmd = new SqlCommand("dbo.st_truncate_table", connection)
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = CommandTimeout
                })
                {
                    try
                    {
                        if (is_stagging)
                        {
                            foreach (var t in listtable)
                            {
                                cmd.Parameters.Clear();
                                cmd.Parameters.AddWithValue("@Tablename", $"[MyDB].[TB_STAGING_{t}]");
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        logger.Error(new SecurityException("SQL Statement Error"), "Error in Sqlstatement (truncate tables)");
                    }
                }
            }
        }

        static void Bulkinsertdynamic(DataSet dataset, DataTable datatable, string dbserver, string database, string rootnode)
        {
            string[] listtable = Helpers.ReadListConfiguration(datatable, "Bulkinsert", rootnode);
            string[] listmappedcolumn = Helpers.ReadListConfiguration(datatable, "mapping", rootnode);
            string CurrentTableName = string.Empty;

            using (SqlConnection connection = new SqlConnection(ConfigurationManager.ConnectionStrings["RallyDB"].ConnectionString))
            {
                connection.Open();
                using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connection)
                {
                    BulkCopyTimeout = CommandTimeout
                })
                {
                    try
                    {
                        DataTable inputDataTableMapping = new DataTable();
                        DataTable ResultDataTableMapping = new DataTable();
                        
                        for (int count = 0; count <= dataset.Tables.Count - 1; count++)
                        {
                            if (listtable.Contains(dataset.Tables[count].TableName))
                            {
                                inputDataTableMapping = dataset.Tables[count];
                                CurrentTableName = dataset.Tables[count].TableName;
                                
                                if (count == 0)
                                {
                                    ResultDataTableMapping = Helpers.CompareRows(
                                        Helpers.GetSQLTable(dbserver, database, "TB_STAGING_" + rootnode.ToUpper()), 
                                        Helpers.GetAGTable(inputDataTableMapping), 
                                        inputDataTableMapping, 
                                        listmappedcolumn);
                                    bulkcopy.DestinationTableName = $"[MyDB].[TB_STAGING_{rootnode.ToUpper()}]";
                                }
                                else
                                {
                                    ResultDataTableMapping = Helpers.CompareRows(
                                        Helpers.GetSQLTable(dbserver, database, $"TB_STAGING_{rootnode.ToUpper()}_{inputDataTableMapping}"),
                                        Helpers.GetAGTable(inputDataTableMapping),
                                        inputDataTableMapping,
                                        listmappedcolumn);
                                    bulkcopy.DestinationTableName = $"[MyDB].[TB_STAGING_{rootnode.ToUpper()}_{inputDataTableMapping.TableName}]";
                                }

                                bulkcopy.ColumnMappings.Clear();
                                foreach (DataColumn column in ResultDataTableMapping.Columns)
                                {
                                    try
                                    {
                                        bulkcopy.ColumnMappings.Add(column.ColumnName.Trim(), column.ColumnName.Trim());
                                    }
                                    catch (Exception e)
                                    {
                                        logger.Error(new SecurityException("Bulk Insert Mapping Error"), 
                                            $"Table: {rootnode} -- Error in BulkInsert");
                                    }
                                }

                                try
                                {
                                    bulkcopy.WriteToServer(ResultDataTableMapping);
                                }
                                catch (Exception e)
                                {
                                    logger.Error(new SecurityException("Bulk Insert Error"), 
                                        $"Error in bulkcopy: {SecurityElement.Escape(e.Message)}");
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        logger.Error(new SecurityException("Bulk Insert Process Error"), 
                            $"Table: {rootnode} -- An error occurred in bulkinsert");
                    }
                }
            }
        }

        static void UpdateDatatable()
        {
            string dbserver = ConfigurationManager.AppSettings["dbserver"];
            string database = ConfigurationManager.AppSettings["database"];
            string ConfigTable = ConfigurationManager.AppSettings["configtable"];
            DataTable ConfigTabletoLoad = Helpers.LoadConfiguration(dbserver, database, ConfigTable);

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["RallyDB"].ConnectionString))
            {
                DataTable dataTable = new DataTable();
                using (SqlCommand cmd = new SqlCommand("SELECT * FROM [MyDB].[TB_STAGING_FEATURE_RevisionHistory]", conn))
                {
                    conn.Open();
                    using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                    {
                        da.Fill(dataTable);
                    }
                }

                int counter = 0;
                foreach (DataRow dr in dataTable.Rows)
                {
                    string url = $"{dr["_ref"]}/Revisions?query=(RevisionNumber = \"0\")";
                    string json = Helpers.WebRequestWithToken(httpclient, url);
                    
                    if (!string.IsNullOrEmpty(json))
                    {
                        JObject rss = JObject.Parse(json);
                        string FeatureCreatorID = (string)rss.SelectToken("QueryResult.Results[0].User._refObjectUUID");
                        
                        if (!string.IsNullOrEmpty(FeatureCreatorID))
                        {
                            dr.BeginEdit();
                            dr["_type"] = SecurityElement.Escape(FeatureCreatorID);
                            dr.EndEdit();
                            counter++;
                        }
                    }
                }

                using (SqlCommand cmd = new SqlCommand("dbo.st_truncate_table", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@Tablename", "[MyDB].[TB_STAGING_FEATURE_RevisionHistory]");
                    cmd.ExecuteNonQuery();

                    using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn))
                    {
                        bulkcopy.DestinationTableName = "[MyDB].[TB_STAGING_FEATURE_RevisionHistory]";
                        bulkcopy.WriteToServer(dataTable);
                    }
                }
            }

            logger.Info($"# {counter} Feature IDs were created");
        }

       static void GetMilestones(string table, string PortfolioItemType)
        {
            string dbserver = ConfigurationManager.AppSettings["dbserver"];
            string database = ConfigurationManager.AppSettings["database"];
            string ConfigTable = ConfigurationManager.AppSettings["configtable"];
            DataTable ConfigTabletoLoad = Helpers.LoadConfiguration(dbserver, database, ConfigTable);

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["RallyDB"].ConnectionString))
            {
                DataTable MilestonesRef = new DataTable();
                using (SqlCommand cmd = new SqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = @"SELECT M._ref as url, P.ObjectUUID as PortfolioItemID 
                                      FROM [MyDB].[TB_STAGING_" + table.ToUpper() + "_Milestones] M 
                                      INNER JOIN [MyDB].[TB_STAGING_" + table.ToUpper() + "] P 
                                      ON (P.Results_Id = M.Results_Id) 
                                      WHERE count > 0";
                    conn.Open();
                    using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                    {
                        da.Fill(MilestonesRef);
                    }
                }

                DataSet dataset = new DataSet();
                bool FirstTime = true;
                foreach (DataRow dr in MilestonesRef.Rows)
                {
                    string url = dr["url"].ToString();
                    string json = Helpers.WebRequestWithToken(httpclient, url);

                    if (!string.IsNullOrEmpty(json) && !json.TrimStart().StartsWith("<"))
                    {
                        try
                        {
                            var doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json);
                            using (var reader = new XmlTextReader(new StringReader(doc.OuterXml)))
                            {
                                dataset.ReadXml(reader);
                            }

                            if (FirstTime && dataset.Tables.Contains("Results"))
                            {
                                AddPortfolioColumns(dataset.Tables["Results"], dr["PortfolioItemID"].ToString(), PortfolioItemType);
                                FirstTime = false;
                            }
                            else if (dataset.Tables.Contains("Results"))
                            {
                                UpdatePortfolioData(dataset.Tables["Results"], dr["PortfolioItemID"].ToString(), PortfolioItemType);
                            }

                            Bulkinsertdynamic(dataset, ConfigTabletoLoad, dbserver, database, "milestones");
                            dataset.Clear();
                        }
                        catch (Exception e)
                        {
                            logger.Error(new SecurityException("Milestone Processing Error"), 
                                SecurityElement.Escape(e.Message));
                        }
                    }
                }
            }
            logger.Info("Milestones inserted into SQL");
        }

        private static void AddPortfolioColumns(DataTable table, string portfolioItemId, string portfolioItemType)
        {
            table.Columns.Add(new DataColumn("PortfolioItemID", typeof(string)));
            table.Columns.Add(new DataColumn("PortfolioItemType", typeof(string)));
            UpdatePortfolioData(table, portfolioItemId, portfolioItemType);
        }

        private static void UpdatePortfolioData(DataTable table, string portfolioItemId, string portfolioItemType)
        {
            foreach (DataRow row in table.Rows)
            {
                row["PortfolioItemID"] = SecurityElement.Escape(portfolioItemId);
                row["PortfolioItemType"] = SecurityElement.Escape(portfolioItemType);
            }
        }

        static void GetRevisionHistory(string table)
        {
            string dbserver = ConfigurationManager.AppSettings["dbserver"];
            string database = ConfigurationManager.AppSettings["database"];
            string ConfigTable = ConfigurationManager.AppSettings["configtable"];
            DataTable ConfigTabletoLoad = Helpers.LoadConfiguration(dbserver, database, ConfigTable);

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["RallyDB"].ConnectionString))
            {
                DataTable dataTable = new DataTable();
                using (SqlCommand cmd = new SqlCommand($"Select * from [MyDB].[TB_STAGING_{table.ToUpper()}_RevisionHistory] order by Results_id", conn))
                {
                    conn.Open();
                    using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                    {
                        da.Fill(dataTable);
                    }
                }

                var Revisions = CreateRevisionsTable();
                foreach (DataRow dr in dataTable.Rows)
                {
                    ProcessRevisionHistory(dr, Revisions);
                }

                using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn))
                {
                    bulkcopy.DestinationTableName = $"[MyDB].[TB_STAGING_{table.ToUpper()}_Revisions]";
                    bulkcopy.WriteToServer(Revisions);
                }
            }
            logger.Info($"{table} Revisions table populated.");
        }

        private static DataTable CreateRevisionsTable()
        {
            var Revisions = new DataTable();
            Revisions.Columns.Add(new DataColumn("ObjectUUID", typeof(string)));
            Revisions.Columns.Add(new DataColumn("CreationDate", typeof(string)));
            Revisions.Columns.Add(new DataColumn("Description", typeof(string)));
            return Revisions;
        }

        private static void ProcessRevisionHistory(DataRow dr, DataTable Revisions)
        {
            int pagesize = 2000;
            string url_count = $"{dr["_ref"]}/Revisions?fetch=ObjectUUID&query=((((Description contains \"NOTES changed \") or (Description contains \"NOTES added \")) or (Description contains \"RAG added \")) or (Description contains \"RAG changed \"))&start=1&pagesize=1";
            string url = $"{dr["_ref"]}/Revisions?query=((((Description contains \"NOTES changed \") or (Description contains \"NOTES added \")) or (Description contains \"RAG added \")) or (Description contains \"RAG changed \"))";

            string json_count = Helpers.WebRequestWithToken(httpclient, url_count);
            if (string.IsNullOrEmpty(json_count)) return;

            JObject rss_count = JObject.Parse(json_count);
            int TotalResultCount = (int?)rss_count.SelectToken("QueryResult.TotalResultCount") ?? 0;
            double nbIteration = Math.Ceiling((double)TotalResultCount / pagesize);
            string varObjectUUID = dr["_refObjectUUID"]?.ToString() ?? string.Empty;

            for (int i = 1; i <= nbIteration; i++)
            {
                ProcessRevisionBatch(url, pagesize, i, varObjectUUID, Revisions);
            }
        }

        private static void ProcessRevisionBatch(string baseUrl, int pagesize, int iteration, string objectUUID, DataTable Revisions)
        {
            int start = ((iteration - 1) * pagesize) + 1;
            string url = $"{baseUrl}&start={start}&pagesize={pagesize}";
            string json = Helpers.WebRequestWithToken(httpclient, url);

            if (string.IsNullOrEmpty(json)) return;

            JObject rss = JObject.Parse(json);
            for (int j = 0; j < pagesize; j++)
            {
                var creationDate = (string)rss.SelectToken($"QueryResult.Results[{j}].CreationDate");
                var description = (string)rss.SelectToken($"QueryResult.Results[{j}].Description");

                if (!string.IsNullOrEmpty(description))
                {
                    var row = Revisions.NewRow();
                    row["ObjectUUID"] = SecurityElement.Escape(objectUUID);
                    row["CreationDate"] = SecurityElement.Escape(creationDate);
                    row["Description"] = SecurityElement.Escape(description);
                    Revisions.Rows.Add(row);
                }
            }
        }

        static void Main(string[] args)
        {
            try
            {
                string environment = ConfigurationManager.AppSettings["ENV"];
                string dbserver = ConfigurationManager.AppSettings["dbserver"];
                string database = ConfigurationManager.AppSettings["database"];
                string configtable = ConfigurationManager.AppSettings["configtable"];

                DataTable ConfigTabletoLoad = Helpers.LoadConfiguration(dbserver, database, configtable);
                string[] listtabletotruncate = Helpers.ReadListConfiguration(ConfigTabletoLoad, "dbstatement", "truncate");
                string[] listtableODStoprocess = Helpers.ReadListConfiguration(ConfigTabletoLoad, "dbstatement", "processODS");
                string[] listtabletoload = Helpers.ReadListConfiguration(ConfigTabletoLoad, "load", "load");

                httpclient = Helpers.WebAuthenticationWithToken(
                    ConfigurationManager.ConnectionStrings["RallyDB"].ConnectionString, 
                    "myApp");

                SQLstatement(dbserver, database, listtabletotruncate, true);

                var tasks = new Task[listtabletoload.Length];
                for (int i = 0; i < listtabletoload.Length; i++)
                {
                    var table = listtabletoload[i];
                    DataSet SQLTargetSet = new DataSet();
                    tasks[i] = Task.Factory.StartNew(() => 
                    {
                        LoadDataFromAgile(SQLTargetSet, dbserver, database, ConfigTabletoLoad, table);
                    }, TaskCreationOptions.LongRunning);
                }

                Task.WaitAll(tasks);
                LoadFeatureException(dbserver, database, "feature", ConfigTabletoLoad);
                logger.Info("Load into SQL Server Staging has been completed");
            }
            catch (Exception e)
            {
                logger.Error(new SecurityException("Main Execution Error"), 
                    SecurityElement.Escape(e.Message));
            }
            finally
            {
                httpclient?.Dispose();
            }
        }
    }
}
