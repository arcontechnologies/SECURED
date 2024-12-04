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


namespace Rally
{

    class RallyLoad
    {
        public static HttpClient httpclient;
        public static Logger logger = LogManager.GetCurrentClassLogger();

        static void SQLstatement(string dbserver, string database, string[] listtable, bool is_stagging)
        {

            var connectionString = Helpers.GetConnectionString(dbserver, database);
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                SqlCommand cmd = new SqlCommand
                {
                    Connection = connection
                };
                try
                {
                    if (is_stagging == true)
                    {
                        foreach (var t in listtable)
                        {
                            cmd.CommandText = "dbo.st_truncate_table";
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.CommandTimeout = 0;
                            cmd.Parameters.Clear();
                            cmd.Parameters.Add(new SqlParameter("@Tablename", "[MyDB].[TB_STAGING_" + t.ToString() + "]"));
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                catch (Exception e)
                {
                    logger.Info(e, "Error in Sqlstatement (truncate tables) : ");
                }

                connection.Close();
            }
        }

        static void Bulkinsertdynamic(DataSet dataset, DataTable datatable, string dbserver, string database, string rootnode)
        {
            string[] listtable = Helpers.ReadListConfiguration(datatable, "Bulkinsert", rootnode);
            string[] listmappedcolumn = Helpers.ReadListConfiguration(datatable, "mapping", rootnode);
            string CurrentTableName = string.Empty;

            string connString = Helpers.GetConnectionString(dbserver, database);
            using (SqlConnection connection = new SqlConnection(connString))
            {
                connection.Open();
                SqlBulkCopy bulkcopy = new SqlBulkCopy(connString)
                {
                    BulkCopyTimeout = 0
                };
                int i = 0;

                try
                {
                    DataTable inputDataTableMapping = new DataTable();
                    DataTable ResultDataTableMapping = new DataTable();
                    for (int count = 0; count <= dataset.Tables.Count - 1; count++)
                    {

                        if (listtable.Contains(dataset.Tables[count].TableName.ToString()))
                        {
                            inputDataTableMapping = dataset.Tables[count];
                            CurrentTableName = dataset.Tables[count].TableName;
                            if (i == 0)
                            {
                                ResultDataTableMapping = Helpers.CompareRows(Helpers.GetSQLTable(dbserver, database, "TB_STAGING_" + rootnode.ToUpper()), Helpers.GetAGTable(inputDataTableMapping), inputDataTableMapping, listmappedcolumn);
                                bulkcopy.DestinationTableName = "[MyDB].[TB_STAGING_" + rootnode.ToUpper() + "]";
                            }
                            else
                            {
                                ResultDataTableMapping = Helpers.CompareRows(Helpers.GetSQLTable(dbserver, database, "TB_STAGING_" + rootnode.ToUpper() + "_" + inputDataTableMapping), Helpers.GetAGTable(inputDataTableMapping), inputDataTableMapping, listmappedcolumn);
                                bulkcopy.DestinationTableName = "[MyDB].[TB_STAGING_" + rootnode.ToUpper() + "_" + inputDataTableMapping.TableName.ToString() + "]";
                            }

                            bulkcopy.ColumnMappings.Clear();

                            int length = ResultDataTableMapping.Columns.Count;

                            for (int k = length - 1; k >= 0; k--)
                            {
                                try
                                {
                                    bulkcopy.ColumnMappings.Add(ResultDataTableMapping.Columns[k].ColumnName.Trim(), ResultDataTableMapping.Columns[k].ColumnName.Trim());
                                }
                                catch (Exception e)
                                {
                                    logger.Info(e, "table : {0} -- Error in BulkInsert : ", rootnode);
                                }
                            }
                            try
                            {
                                bulkcopy.WriteToServer(ResultDataTableMapping);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("Error in bulkcopy : {0} ", e);
                                logger.Error(e, "Error in bulkcopy : ");
                            }



                            i++;

                        }
                    }
                }
                catch (Exception e)
                {
                    logger.Error(e, "table : {0} -- An error occured in bulkinsert : ", rootnode);
                }
                bulkcopy.Close();
                connection.Close();
            }
        }

        // retrieve feature creator name from feature revision history

        static void UpdateDatatable()
        {
            string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
            string database = ConfigurationManager.AppSettings["database"].ToString();
            string ConfigTable = ConfigurationManager.AppSettings["configtable"].ToString();

            DataTable ConfigTabletoLoad = Helpers.LoadConfiguration(dbserver, database, ConfigTable);

            string json = string.Empty;
            string url;

            // load datatable Feature RevisionHistory
            string connString = Helpers.GetConnectionString(dbserver, database);
            SqlConnection conn = new SqlConnection(connString);

            // insert Revisions datable into SQL table Revisions
            DataTable dataTable = new DataTable();


            using (SqlConnection connection = new SqlConnection(connString))
            {
                string query = "Select * from [MyDB].[TB_STAGING_FEATURE_RevisionHistory]";
                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(dataTable);
                conn.Close();
                da.Dispose();

            }

            int counter = 0;
            foreach (DataRow dr in dataTable.Rows)
            {
                url = dr["_ref"].ToString() + "/Revisions?query=(RevisionNumber = \"0\")";
                json = Helpers.WebRequestWithToken(httpclient, url);
                JObject rss = JObject.Parse(json);
                string FeatureCreatorID = (string)rss.SelectToken("QueryResult.Results[0].User._refObjectUUID");
                if (!String.IsNullOrEmpty(FeatureCreatorID))
                {
                    dr.BeginEdit();
                    dr["_type"] = FeatureCreatorID.ToString();
                    dr.EndEdit();
                    counter++;
                    //Console.Write("# Feature creators ID: \r{0} ", counter);
                }

            }

            using (SqlConnection connection = new SqlConnection(connString))
            {
                conn.Open();

                SqlCommand cmd = new SqlCommand()
                {
                    Connection = conn,
                    CommandText = "dbo.st_truncate_table",
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = 0
                };
                cmd.Parameters.Clear();
                cmd.Parameters.Add(new SqlParameter("@Tablename", "[MyDB].[TB_STAGING_FEATURE_RevisionHistory]"));
                cmd.ExecuteNonQuery();


                SqlBulkCopy bulkcopy = new SqlBulkCopy(conn)
                {
                    DestinationTableName = "[MyDB].[TB_STAGING_FEATURE_RevisionHistory]"
                };
                bulkcopy.WriteToServer(dataTable);
                dataTable.Clear();
                conn.Close();
            }
            // Console.WriteLine("Feature Revisions table populated with feature creator ID.");
            logger.Info("# {0} Feature IDs were created", counter);

        }


        static void GetMilestones(string table, string PortfolioItemType)
        {
            string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
            string database = ConfigurationManager.AppSettings["database"].ToString();
            string ConfigTable = ConfigurationManager.AppSettings["configtable"].ToString();

            DataTable ConfigTabletoLoad = Helpers.LoadConfiguration(dbserver, database, ConfigTable);


            string json = string.Empty;
            string url;

            // load datatable Feature RevisionHistory
            string connString = Helpers.GetConnectionString(dbserver, database);
            SqlConnection conn = new SqlConnection(connString);

            // insert Revisions datable into SQL table Revisions
            DataTable MilestonesRef = new DataTable();


            using (SqlConnection connection = new SqlConnection(connString))
            {
                string query = "SELECT M._ref as url, P.ObjectUUID as PortfolioItemID FROM [MyDB].[TB_STAGING_" + table.ToUpper() + "_Milestones] M " +
                               "INNER JOIN [MyDB].[TB_STAGING_" + table.ToUpper() + "] P ON (P.Results_Id = M.Results_Id) " +
                               "WHERE count > 0";
                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(MilestonesRef);
                conn.Close();
                da.Dispose();

            }

            DataSet dataset = new DataSet();
            bool FirstTime = true;
            foreach (DataRow dr in MilestonesRef.Rows)
            {
                int currentIndex = 0;

                url = dr["url"].ToString();
                //json = Helpers.WebRequestWithCredentials(url,credentials);
                json = Helpers.WebRequestWithToken(httpclient, url);
                try
                {
                    if (json.TrimStart().StartsWith("<") == false)
                    {
                        XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json);
                        json = string.Empty;

                        foreach (DataTable dt in dataset.Tables)
                            dt.BeginLoadData();

                        dataset.ReadXml(new XmlTextReader(new StringReader(doc.OuterXml)));

                        foreach (DataTable dt in dataset.Tables)
                            dt.EndLoadData();

                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("An error occurred in load Datasets - DeserializeXmlNode : '{0}'", e);
                   
                }

                if (FirstTime)
                {
                    DataColumn NewCol;

                    NewCol = new DataColumn
                    {
                        ColumnName = "PortfolioItemID",
                        DataType = System.Type.GetType("System.String")
                    };
                    dataset.Tables["Results"].Columns.Add(NewCol);
                    NewCol = new DataColumn
                    {
                        ColumnName = "PortfolioItemType",
                        DataType = System.Type.GetType("System.String")
                    };
                    dataset.Tables["Results"].Columns.Add(NewCol);

                    foreach (DataRow rw in dataset.Tables["Results"].Rows)
                    {
                        dataset.Tables["Results"].Rows[currentIndex]["PortfolioItemID"] = dr["PortfolioItemID"];
                        dataset.Tables["Results"].Rows[currentIndex]["PortfolioItemType"] = PortfolioItemType;
                        currentIndex++;
                    }

                    FirstTime = false;
                }
                else
                {
                    foreach (DataRow rw in dataset.Tables["Results"].Rows)
                    {
                        dataset.Tables["Results"].Rows[currentIndex]["PortfolioItemID"] = dr["PortfolioItemID"];
                        dataset.Tables["Results"].Rows[currentIndex]["PortfolioItemType"] = PortfolioItemType;
                        currentIndex++;
                    }
                }

                Bulkinsertdynamic(dataset, ConfigTabletoLoad, dbserver, database, "milestones");
                dataset.Clear();


            }
            // Console.WriteLine("Feature Revisions table populated with feature creator ID.");
            logger.Info("Milestones inserted into SQL");

        }

        // retrieve selected revision history items from epic revision history

        static void GetRevisionHistory(string table)
        {
            string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
            string database = ConfigurationManager.AppSettings["database"].ToString();

            string ConfigTable = ConfigurationManager.AppSettings["configtable"].ToString();

            DataTable ConfigTabletoLoad = Helpers.LoadConfiguration(dbserver, database, ConfigTable);

            //string credentials = Helpers.ReadConfiguration(ConfigTabletoLoad, "url", "credentials");


            string json = string.Empty;
            string url;
            string url_count;
            int pagesize = 2000;

            // load datatable Epic RevisionHistory
            string connString = Helpers.GetConnectionString(dbserver, database);
            SqlConnection conn = new SqlConnection(connString);

            // insert Revisions datable into SQL table Revisions
            DataTable dataTable = new DataTable();

            DataTable opus = new DataTable();

            using (SqlConnection connection = new SqlConnection(connString))
            {
                string query = "Select * from [MyDB].[TB_STAGING_" + table.ToUpper() + "] order by Results_id";
                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(opus);
                conn.Close();
                da.Dispose();

            }


            using (SqlConnection connection = new SqlConnection(connString))
            {
                string query = "Select * from [MyDB].[TB_STAGING_" + table.ToUpper() + "_RevisionHistory] order by Results_id";
                SqlCommand cmd = new SqlCommand(query, conn);
                conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(dataTable);
                conn.Close();
                da.Dispose();

            }



            DataTable Revisions = new DataTable();
            DataColumn NewCol;

            NewCol = new DataColumn
            {
                ColumnName = "ObjectUUID",
                DataType = System.Type.GetType("System.String")
            };
            Revisions.Columns.Add(NewCol);

            NewCol = new DataColumn
            {
                ColumnName = "CreationDate",
                DataType = System.Type.GetType("System.String")
            };
            Revisions.Columns.Add(NewCol);

            NewCol = new DataColumn
            {
                ColumnName = "Description",
                DataType = System.Type.GetType("System.String")
            };
            Revisions.Columns.Add(NewCol);

            foreach (DataRow dr in dataTable.Rows)
            {
                int CurrentResultID = Convert.ToInt32(dr["Results_Id"]);
                  url_count = dr["_ref"].ToString() + "/Revisions?fetch=ObjectUUID&query=((((Description contains \"NOTES changed \") or (Description contains \"NOTES added \")) or (Description contains \"RAG added \")) or (Description contains \"RAG changed \"))&start=1&pagesize=1";
                url = dr["_ref"].ToString() + "/Revisions?query=((((Description contains \"NOTES changed \") or (Description contains \"NOTES added \")) or (Description contains \"RAG added \")) or (Description contains \"RAG changed \"))";


                string json_count = Helpers.WebRequestWithToken(httpclient, url_count);
                //string json_count = Helpers.WebRequestWithCredentials(url, credentials);

                JObject rss_count = JObject.Parse(json_count);
                //string[] Error = (string)rss_count.SelectToken("QueryResult.Errors");
                int TotalResultCount = 0;
                if (rss_count.SelectToken("QueryResult.TotalResultCount") != null)
                {
                    TotalResultCount = (int)rss_count.SelectToken("QueryResult.TotalResultCount");
                }

                double nbIteration = Math.Ceiling((double)TotalResultCount / pagesize);

                string varObjectUUID = opus.Rows[CurrentResultID]["_refObjectUUID"].ToString();

                int start = 1;

                for (int i = 1; i <= nbIteration; i++)
                {
                    url = url + "&start=" + start + "&pagesize=" + pagesize;
                    //json = Helpers.WebRequestWithCredentials(url, credentials);
                    json = Helpers.WebRequestWithToken(httpclient, url);
                    JObject rss = JObject.Parse(json);
                    //string[] tokens = json.Split(',');

                    string varCreationDate = string.Empty;
                    string varDescription = string.Empty;

                    for (int j = 0; j < TotalResultCount; j++)
                    {
                        DataRow NewRow = Revisions.NewRow();
                        varCreationDate = (string)rss.SelectToken("QueryResult.Results[" + j.ToString() + "].CreationDate");
                        varDescription = (string)rss.SelectToken("QueryResult.Results[" + j.ToString() + "].Description");
                        if (varDescription != null)
                        {
                            NewRow["ObjectUUID"] = varObjectUUID;
                            NewRow["CreationDate"] = varCreationDate;
                            NewRow["Description"] = varDescription;
                            Revisions.Rows.Add(NewRow);
                        }
                    }
                    start = start + pagesize;
                }
                //}
            }

            // insert Revisions datable into SQL table Revisions
            using (SqlConnection connection = new SqlConnection(connString))
            {
                conn.Open();
                SqlBulkCopy bulkcopy = new SqlBulkCopy(conn)
                {
                    DestinationTableName = "[MyDB].[TB_STAGING_" + table.ToUpper() + "_Revisions]"
                };
                bulkcopy.WriteToServer(Revisions);
                Revisions.Clear();
                conn.Close();
            }
            //Console.WriteLine("Revisions table populated.");
            logger.Info("Opus Revisions table populated.");
        }

        static void RestoreColumnNames(DataSet dataset, string table)
        {
            if (dataset.Tables.Contains("Results") && table == "feature")
            {
                if (dataset.Tables["Results"].Columns.Contains("c_PI"))
                {
                    dataset.Tables["Results"].Columns["c_PI"].ColumnName = "C_PILOTPI";

                }

                if (dataset.Tables["Results"].Columns.Contains("c_PIREFID"))
                {
                    dataset.Tables["Results"].Columns["c_PIREFID"].ColumnName = "C_PILOTPIREFID";
                }
            }

            if (dataset.Tables.Contains("Results") && table == "userstory")
            {
                if (dataset.Tables["Results"].Columns.Contains("c_GOS"))
                {
                    dataset.Tables["Results"].Columns["c_GOS"].ColumnName = "c_PilotGOS";
                }
            }
        }

        public static void LoadFeatureException(string dbserver, string database, string table, DataTable ConfigTable)
        {
            string connString = Helpers.GetConnectionString(dbserver, database);
            SqlConnection conn = new SqlConnection(connString);
            string urlbase = Helpers.ReadConfiguration(ConfigTable, "url", table);

            int AddDays = Convert.ToInt32(ConfigurationManager.AppSettings["AddDays"].ToString());
            string LastUpdateDate = DateTime.Today.AddDays(AddDays * -1).ToString("yyyy-MM-dd");

            string json = string.Empty;
            string url = urlbase + "&query=((LastUpdateDate >= " + LastUpdateDate + ") AND (c_RequiredDeliveryDate != null))"; ;
            int pagesize = 2000;

            string json_count = Helpers.WebRequestWithToken(httpclient, url);
            JObject rss_count = JObject.Parse(json_count);
            int TotalResultCount = (int)rss_count.SelectToken("QueryResult.TotalResultCount");

            double nbIteration = Math.Ceiling((double)TotalResultCount / pagesize);

            DataTable DTexception = new DataTable();
            DataColumn NewCol;

            NewCol = new DataColumn
            {
                ColumnName = "_rallyAPIMajor",
                DataType = System.Type.GetType("System.String")
            };
            DTexception.Columns.Add(NewCol);

            NewCol = new DataColumn
            {
                ColumnName = "_rallyAPIMinor",
                DataType = System.Type.GetType("System.String")
            };
            DTexception.Columns.Add(NewCol);

            int start = 1;
            url = urlbase;
            for (int i = 1; i <= nbIteration; i++)
            {
                url = url + "&fetch=FormattedID,c_RequiredDeliveryDate&start=" + start + "&pagesize=" + pagesize + "&query=((LastUpdateDate >= " + LastUpdateDate + ") AND (c_RequiredDeliveryDate != null))";
                json = Helpers.WebRequestWithToken(httpclient, url);
                JObject rss = JObject.Parse(json);

                string FormattedID = string.Empty;
                string c_RequiredDeliveryDate = string.Empty;

                for (int j = 0; j < TotalResultCount; j++)
                {
                    DataRow NewRow = DTexception.NewRow();
                    FormattedID = (string)rss.SelectToken("QueryResult.Results[" + j.ToString() + "].FormattedID");
                    c_RequiredDeliveryDate = (string)rss.SelectToken("QueryResult.Results[" + j.ToString() + "].c_RequiredDeliveryDate");
                    if (FormattedID != null)
                    {
                        NewRow["_rallyAPIMajor"] = FormattedID;
                        NewRow["_rallyAPIMinor"] = c_RequiredDeliveryDate;
                        DTexception.Rows.Add(NewRow);
                    }
                }
                start = start + pagesize;
                url = urlbase;
            }

            // insert Revisions datable into SQL table Revisions
            using (SqlConnection connection = new SqlConnection(connString))
            {

                conn.Open();

                // truncate ExpertiseDemands table
                SqlCommand cmd = new SqlCommand()
                {
                    Connection = conn,
                    CommandText = "dbo.st_truncate_table",
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = 0
                };
                cmd.Parameters.Clear();
                cmd.Parameters.Add(new SqlParameter("@Tablename", "[MyDB].[TB_STAGING_FEATURE_ExpertiseDemands]"));
                cmd.ExecuteNonQuery();

                // // truncate ExpertiseDemands table
                SqlBulkCopy bulkcopy = new SqlBulkCopy(conn)
                {
                    DestinationTableName = "[MyDB].[TB_STAGING_" + table.ToUpper() + "_ExpertiseDemands]"
                };
                bulkcopy.WriteToServer(DTexception);
                DTexception.Clear();

                cmd = new SqlCommand()
                {
                    Connection = conn,
                    CommandText = "UPDATE [MyDB].[TB_STAGING_FEATURE] SET c_Businesswishdate = S._rallyAPIMinor FROM [MyDB].[TB_STAGING_FEATURE_ExpertiseDemands] S INNER JOIN [MyDB].[TB_STAGING_FEATURE] T ON T.FormattedID = S._rallyAPIMajor",
                    CommandType = CommandType.Text,
                    CommandTimeout = 0
                };
                cmd.Parameters.Clear();
                cmd.ExecuteNonQuery();
                conn.Close();
            }
        }

        static void LoadDataset(DataSet dataset, DataTable ConfigTable, string table, string table_count, int pagesize)
        {
            string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
            string database = ConfigurationManager.AppSettings["database"].ToString();
            int AddDays = Convert.ToInt32(ConfigurationManager.AppSettings["AddDays"].ToString());

            string url_count = Helpers.ReadConfiguration(ConfigTable, "url", table_count);
            string urlbase = Helpers.ReadConfiguration(ConfigTable, "url", table);
            string credentials = Helpers.ReadConfiguration(ConfigTable, "url", "credentials");

            string json = string.Empty;
            // StringBuilder json;
            //int pagesize = 500;

            string LastUpdateDate = DateTime.Today.AddDays(AddDays * -1).ToString("yyyy-MM-dd");


            if (table == "userstory" || table == "feature")
            {
                url_count = url_count + "&fetch=ObjectUUID&start=1&pagesize=1&query=(LastUpdateDate >= " + LastUpdateDate + ")";
            }

            if (table == "iteration")
            {
                url_count = url_count + "&fetch=ObjectUUID&start=1&pagesize=1&query=(CreationDate >= " + LastUpdateDate + ")";
            }
            //string json_count = Helpers.WebRequestWithCredentials(url_count,credentials);
            string json_count = Helpers.WebRequestWithToken(httpclient, url_count);
            string[] tokens = json_count.Split(',');
            int TotalResultCount = Convert.ToInt32(Regex.Match(tokens[4], @"\d+").Value);
            //int TotalResultCount = 1501;
            double nbIteration = Math.Ceiling((double)TotalResultCount / pagesize);

            int start = 1;
            //maximum can be loaded via API 2.0
            string url = urlbase;

            try
            {

                for (int i = 1; i <= nbIteration; i++)
                {

                    if (table == "userstory" || table == "feature")
                    {
                        url = url + "&fetch=true&start=" + start + "&pagesize=" + pagesize + "&query=(LastUpdateDate >= " + LastUpdateDate + ")";
                    }
                    else
                    {
                        if (table == "iteration")
                        {
                            url = url + "&fetch=true&start=" + start + "&pagesize=" + pagesize + "&query=(CreationDate >= " + LastUpdateDate + ")";
                        }
                        else
                        {
                            url = url + "&start=" + start + "&pagesize=" + pagesize;
                        }
                    }

                    json = Helpers.WebRequestWithToken(httpclient, url);
                    try
                    {
                        if (json.TrimStart().StartsWith("<") == false)
                        {
                            XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json);
                            json = string.Empty;

                            foreach (DataTable dataTable in dataset.Tables)
                                dataTable.BeginLoadData();

                            dataset.ReadXml(new XmlTextReader(new StringReader(doc.OuterXml)));

                            foreach (DataTable dataTable in dataset.Tables)
                                dataTable.EndLoadData();

                            url = urlbase;
                            start = start + pagesize;
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("An error occurred in load Datasets - DeserializeXmlNode : '{0}'", e);
                    }

                    // Bulkinsert SQL Tables

                    if (table == "risk")
                    {
                        var rows = dataset.Tables["_tagsNameArray"].Select("Tags_Id is null");
                        if (rows.Count() > 0)
                        {
                            foreach (var row in rows)
                            { row.Delete(); }
                            dataset.Tables["_tagsNameArray"].AcceptChanges();
                        }
                    }

                    Bulkinsertdynamic(dataset, ConfigTable, dbserver, database, table.ToString());
                    if (table.ToString() == "opus")
                    {
                        DataSet EpicDataset = new DataSet();
                        EpicDataset = dataset.Copy();
                        Bulkinsertdynamic(EpicDataset, ConfigTable, dbserver, database, "epic");
                    }


                    dataset.Clear();

                    if (table == "feature")
                    {
                        dataset.Tables["Results"].Columns["c_zREMOVE2ClarityID"].ColumnName = "c_PERFTest";
                        dataset.Tables["Results"].Columns["c_zREMOVEBNPPFeatureType"].ColumnName = "c_WAVATest";
                    }

                    if (table == "opus")
                    {
                        dataset.Tables["Results"].Columns["c_PRMForecastDeliveryDate"].ColumnName = "c_PRMDeliveryDate";
                        dataset.Tables["Results"].Columns["c_Businesswishdate"].ColumnName = "c_RequiredDeliveryDate";
                    }
                }
            }
            catch (Exception e)
            {
                //Console.WriteLine("An error occurred in LoadDataset: '{0}'", e);

                logger.Error(e, "table : {0} -- An error occured in LoadDataset : ", table);
            }
            finally
            {
                if (dataset.Tables.Contains("RevisionHistory") && table_count == "feature_count")
                {
                    // add feature creator into Revisionhistory
                    GetMilestones("feature", "feature");
                    UpdateDatatable();
                }
                if (dataset.Tables.Contains("RevisionHistory"))
                {
                    if (table_count == "opus_count")
                    {
                        GetRevisionHistory("opus");
                    }
                    else if (table_count == "initiative_count")
                    {
                        GetRevisionHistory("initiative");
                    }
                }

            }
        }

        static void LoadDatasetExceptionUserstory(DataSet dataset, DataTable ConfigTable, string table, string table_count)
        {

            int AddDays = Convert.ToInt32(ConfigurationManager.AppSettings["AddDays"].ToString());
            string LastUpdateDate = DateTime.Today.AddDays(AddDays * -1).ToString("yyyy-MM-dd");

            string url_count = "https://eu1.rallydev.com/slm/webservice/v2.0/hierarchicalrequirement?workspace=https://eu1.rallydev.com/slm/webservice/v2.0/workspace/99999999999&query=((Parent%20!=%20null) AND (LastUpdateDate >= " + LastUpdateDate + "))&fetch=TotalResultCount&start=1&pagesize=1";
            string urlbase = "https://eu1.rallydev.com/slm/webservice/v2.0/hierarchicalrequirement?workspace=https://eu1.rallydev.com/slm/webservice/v2.0/workspace/99999999999&query=((Parent%20!=%20null) AND (LastUpdateDate >= " + LastUpdateDate + "))&fetch=ObjectUUID,Parent";
            string json = string.Empty;
            int pagesize = 2000;

            string credentials = Helpers.ReadConfiguration(ConfigTable, "url", "credentials");
            //string json_count = Helpers.WebRequestWithCredentials(url_count,credentials);
            string json_count = Helpers.WebRequestWithToken(httpclient, url_count);
            string[] tokens = json_count.Split(',');
            int TotalResultCount = Convert.ToInt32(Regex.Match(tokens[4], @"\d+").Value);
            double nbIteration = Math.Ceiling((double)TotalResultCount / pagesize);

            int start = 1;
            //maximum can be loaded via API 2.0
            string url = urlbase;

            try
            {
                for (int i = 1; i <= nbIteration; i++)
                {
                    url = url + "&start=" + start + "&pagesize=" + pagesize;

                    //json = Helpers.WebRequestWithCredentials(url,credentials);

                    json = Helpers.WebRequestWithToken(httpclient, url);
                    try
                    {
                        if (json.TrimStart().StartsWith("<") == false)
                        {
                            XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json);
                            json = string.Empty;

                            foreach (DataTable dataTable in dataset.Tables)
                                dataTable.BeginLoadData();

                            dataset.ReadXml(new XmlTextReader(new StringReader(doc.OuterXml)));

                            foreach (DataTable dataTable in dataset.Tables)
                                dataTable.EndLoadData();

                            url = urlbase;
                            start = start + pagesize;
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("An error occurred in load Datasets - DeserializeXmlNode : '{0}'", e);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred in LoadDataset: '{0}'", e);
            }
            finally
            {
                // enrich Parent table

                string[] dest_Columns = { "_rallyAPIMajor", "_rallyAPIMinor", "_ref", "_refObjectUUID", "_refObjectName", "_type", "Results_Id" };
                DataTable dt_insert = new DataTable();

                foreach (DataTable dt in dataset.Tables)
                {
                    if (dt.TableName.ToString() == "Parent")
                    {
                        int i = 0;
                        foreach (DataRow rw in dt.Rows)
                        {
                            //Console.WriteLine(" ObjectUUID : {0}", dataset.Tables["Results"].Rows[i].Field<string>("ObjectUUID").ToString());
                            rw["_ref"] = dataset.Tables["Results"].Rows[i].Field<string>("ObjectUUID").ToString();
                            i++;
                        }
                        dt_insert = dt.Copy();
                    }
                }

                // bulkinsert Parent table

                string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
                string database = ConfigurationManager.AppSettings["database"].ToString();
                string connString = Helpers.GetConnectionString(dbserver, database);
                SqlConnection conn = new SqlConnection(connString);
                using (SqlConnection connection = new SqlConnection(connString))
                {
                    conn.Open();

                    SqlBulkCopy bulkcopy = new SqlBulkCopy(conn)
                    {
                        DestinationTableName = "[MyDB].[TB_STAGING_USERSTORY_Parent]"
                    };

                    // Column Mapping
                    bulkcopy.ColumnMappings.Clear();
                    int length = dt_insert.Columns.Count;
                    for (int k = length - 1; k >= 0; k--)
                    {
                        try
                        {
                            if (dest_Columns.Contains(dt_insert.Columns[k].ColumnName.Trim()))
                            {
                                bulkcopy.ColumnMappings.Add(dt_insert.Columns[k].ColumnName.Trim(), dt_insert.Columns[k].ColumnName.Trim());
                            }
                        }
                        catch (Exception e)
                        {
                            logger.Info(e, "Error in columnmapping : ");
                        }
                    }

                    // Bulk Insert
                    bulkcopy.WriteToServer(dt_insert);
                    dt_insert.Clear();
                    conn.Close();
                }
                //Console.WriteLine("UserStory Parent Table filled");
                logger.Info("UserStory Parent Table filled.");

            }
        }


        static void MakelinksforAffectedItemsforRisk(string dbserver, string database)
        {

            string ConfigTable = ConfigurationManager.AppSettings["configtable"].ToString();

            DataTable ConfigTabletoLoad = Helpers.LoadConfiguration(dbserver, database, ConfigTable);

            string credentials = Helpers.ReadConfiguration(ConfigTabletoLoad, "url", "credentials");

            string connString = Helpers.GetConnectionString(dbserver, database);
            DataTable RefLinks = new DataTable();

            string query = "select * from TB_STAGING_RISK_WorkItemsAffected" + " where [Count] > 0 order by [Results_Id]";
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(query, conn);
            conn.Open();
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            da.Fill(RefLinks);
            conn.Close();
            da.Dispose();
            query = string.Empty;

            DataTable RefTargetToLoad = new DataTable();
            DataColumn NewCol;
            DataRow NewRow;

            NewCol = new DataColumn
            {
                ColumnName = "ObjectIDRisk",
                DataType = System.Type.GetType("System.String")
            };
            RefTargetToLoad.Columns.Add(NewCol);

            NewCol = new DataColumn
            {
                ColumnName = "ObjectIDAffectedItem",
                DataType = System.Type.GetType("System.String")
            };
            RefTargetToLoad.Columns.Add(NewCol);

            NewCol = new DataColumn
            {
                ColumnName = "Type",
                DataType = System.Type.GetType("System.String")
            };
            RefTargetToLoad.Columns.Add(NewCol);

            foreach (DataRow row in RefLinks.Rows)
            {

                // Get the ref (URL) and invoque Rest API
                string url = row["_ref"].ToString() + "?fetch=_refObjectUUID&start=1&pagesize=2000";
                //url.Replace("https", "http");
                string ObjectIDRisk = Regex.Match(url, @"[^\d](\d{11})[^\d]").Value;
                ObjectIDRisk = ObjectIDRisk.Replace("/", "");

                //string json = Helpers.WebRequestWithCredentials(url, credentials);
                string json = Helpers.WebRequestWithToken(httpclient, url);
                try
                {
                    if (json.TrimStart().StartsWith("<") == false)
                    {
                        XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json.ToString());
                        DataSet dt = new DataSet();


                        foreach (DataTable dataTable in dt.Tables)
                            dataTable.BeginLoadData();

                        dt.ReadXml(new XmlTextReader(new StringReader(doc.OuterXml)));

                        foreach (DataTable dataTable in dt.Tables)
                            dataTable.EndLoadData();

                        if (dt.Tables["Results"] != null)
                        {
                            int j = 1;
                            foreach (DataRow rwdt in dt.Tables["Results"].Rows)
                            {
                                NewRow = RefTargetToLoad.NewRow();
                                NewRow["ObjectIDRisk"] = ObjectIDRisk;
                                NewRow["ObjectIDAffectedItem"] = rwdt["_refObjectUUID"].ToString();
                                NewRow["Type"] = rwdt["_type"].ToString();
                                RefTargetToLoad.Rows.Add(NewRow);
                                j++;
                            }
                            dt.Clear();
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("An error occurred in load dependencies - DeserializeXmlNode : '{0}'", e);
                }
            }

            RefLinks.Clear();

            using (SqlConnection connection = new SqlConnection(connString))
            {
                conn.Open();
                SqlBulkCopy bulkcopy = new SqlBulkCopy(conn)
                {
                    DestinationTableName = "[MyDB].[TB_STAGING_RISK_LINKS]"
                };
                bulkcopy.WriteToServer(RefTargetToLoad);
                RefTargetToLoad.Clear();
                conn.Close();
            }
            //Console.WriteLine("Links for Table Risk is in process...");
            logger.Info("Links for Table Risk have been created");
        }

        static void LoadDataFromAgile(DataSet SQLTargetSet, string dbserver, string database, DataTable ConfigTable, string table)
        {
            int BlockSize = Convert.ToInt32(ConfigurationManager.AppSettings["BlockSize"].ToString());

            if (table == "userstory")
            {
                Console.WriteLine("task load for {0} table has started", table.ToString());
                logger.Info("task load for {0} table has started", table.ToString());
                LoadDataset(SQLTargetSet, ConfigTable, table.ToString(), table.ToString() + "_count", BlockSize);
                //Bulkinsertdynamic(SQLTargetSet, ConfigTable, dbserver, database, table.ToString());
                Console.WriteLine("task load for {0} table complete", table.ToString());
                logger.Info("task load for {0} table complete", table.ToString());
                SQLTargetSet.Dispose();
                Console.WriteLine("Manage exceptions for {0} table", table.ToString());
                logger.Info("Manage exceptions for {0} table", table.ToString());
                DataSet ExceptionDataset = new DataSet();
                LoadDatasetExceptionUserstory(ExceptionDataset, ConfigTable, table.ToString(), table.ToString() + "_count");
                Console.WriteLine("task load for Parent UserStory done");
                logger.Info("task load for Parent UserStory done");
                //Console.WriteLine("-----------------------------------");
            }
            else
            {
                Console.WriteLine("task load for {0} table has started", table.ToString());
                logger.Info("task load for {0} table has started", table.ToString());

                LoadDataset(SQLTargetSet, ConfigTable, table.ToString(), table.ToString() + "_count", BlockSize);
                //Bulkinsertdynamic(SQLTargetSet, ConfigTable, dbserver, database, table.ToString());
                Console.WriteLine("task load for {0} table complete", table.ToString());
                logger.Info("task load for {0} table complete", table.ToString());

                SQLTargetSet.Dispose();

                if (table == "risk")
                {
                    Console.WriteLine("Make links for affected Items by Risks started");
                    MakelinksforAffectedItemsforRisk(dbserver, database);
                    Console.WriteLine("Make links for affected Items by Risks completed");
                }
            }

        }


        // Main program to trigger the required methods....

        static void Main(string[] args)
        {
 
            string environment = ConfigurationManager.AppSettings["ENV"].ToString();
            string dbserver = ConfigurationManager.AppSettings["dbserver"].ToString();
            string database = ConfigurationManager.AppSettings["database"].ToString();
            string configtable = ConfigurationManager.AppSettings["configtable"].ToString();

            DataTable ConfigTabletoLoad = Helpers.LoadConfiguration(dbserver, database, configtable);

            string[] listtabletotruncate = Helpers.ReadListConfiguration(ConfigTabletoLoad, "dbstatement", "truncate");
            string[] listtableODStoprocess = Helpers.ReadListConfiguration(ConfigTabletoLoad, "dbstatement", "processODS");
            string[] listtabletoload = Helpers.ReadListConfiguration(ConfigTabletoLoad, "load", "load");

            string connString = Helpers.GetConnectionString(dbserver, database);
            httpclient = Helpers.WebAuthenticationWithToken(connString, "myApp");

            SQLstatement(dbserver, database, listtabletotruncate, true);

            Task[] tasks = new Task[listtabletoload.Length];
            int i = 0;
            foreach (var table in listtabletoload)
            {
                DataSet SQLTargetSet = new DataSet();
                tasks[i] = Task.Factory.StartNew(() => LoadDataFromAgile(SQLTargetSet, dbserver, database, ConfigTabletoLoad, table), TaskCreationOptions.LongRunning);
                i++;
            }

            Task.WaitAll(tasks);

            //// Handle temporary exception for feature
            LoadFeatureException(dbserver, database, "feature", ConfigTabletoLoad);
            logger.Info("load into SQL Server Staging has been completed");
            httpclient.Dispose();

        }
    }
}
