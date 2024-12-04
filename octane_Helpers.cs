using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Net.Http.Headers;
using System.Net.Http;
using System.Threading.Tasks;
using System.Configuration;
using System.Net;
using System.Threading;
using System.Collections.Generic;
using DataEncryption;


namespace OctaneSync
{
    internal static class Helpers
    {

        public const string type = "query";
        public const string list = "list";
        public static DataTable ConfigTable { get; set; } = LoadConfiguration(DbServer, Database, ConfigurationManager.AppSettings["configTable"].ToString());
        
        public static readonly string Database = ConfigurationManager.AppSettings["database"].ToString();
        public static readonly string DbServer = ConfigurationManager.AppSettings["dbserver"].ToString();
        public static readonly string OctaneUrl = ConfigurationManager.AppSettings["octaneurl"].ToString();
       
        public static readonly string QueryRallyEpic = ReadConfiguration(ConfigTable, type,"QueryRallyEpic");
        public static readonly string QueryRallyFeature = ReadConfiguration(ConfigTable, type, "QueryRallyFeature");  
        public static readonly string QueryRallyUserStory = ReadConfiguration(ConfigTable, type, "QueryRallyUserStory"); 

        public static string QueryRallyMilestone { get; set; } = ReadConfiguration(ConfigTable, type, "QueryRallyMilestone");  
        public static readonly string QueryRallyMilestoneForFeature = ReadConfiguration(ConfigTable, type, "QueryRallyMilestoneForFeature"); 
        public static readonly string[] Ready_udf_List = ReadConfiguration(ConfigTable, list, "ready_udf_list").Split(';');  

        public static DataTable dtUsers { get; set; } = new DataTable();
        public static DataTable dtMilestonesForFeatures { get; set; } = Helpers.GetDataTableFromSql(Helpers.DbServer, Helpers.Database, Helpers.QueryRallyMilestoneForFeature);

        public static string ClientId { get; set; }
        public static string ClientSecret { get; set; }
        public static string SharedSpaceId { get; set; }
        public static string WorkspaceId { get; set; }
        public static string ParentWorkspaceId { get; set; }

        public const string colMilstone = "milestoneid";
        public static readonly string queryConfig = "SELECT * FROM " + ConfigurationManager.AppSettings["configTable"].ToString();
        public static readonly string message_milestone = "Milestone : {0} for feature {1} added.";

        public static DataTable LoadConfiguration(string dbserver, string database, string configtable)
        {
            DataTable datatable = new DataTable();
            try
            {
                string connString = GetConnectionString(dbserver, database);
                using (SqlConnection conn = new SqlConnection(connString))
                {
                    using (SqlCommand cmd = new SqlCommand(queryConfig, conn))
                    {
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
                Octane.logger.Error(e, "An error occurred in load configuration: {0}");
            }
            return datatable;
        }

        public static string ReadConfiguration(DataTable datatable, string category, string key)
        {
            DataRow[] value;
            string expression = "Category Like '" + category + "' and Key Like '" + key + "'";

            value = datatable.Select(expression);
            return value[0][3].ToString();
        }
        public static void DisplayMessage(string message, params object[] args)
        {
            if (args == null || args.Length == 0)
            {
                if (message == "=" || message == "-" || message == "*" || message == "_")
                {
                    Console.WriteLine(string.Concat(Enumerable.Repeat(message, 100)));
                }
                else
                {
                    Console.WriteLine(message.Replace("[", "").Replace("]", "").Replace("{", "").Replace("}", ""));
                }
            }
            else
            {
                for (int i = 0; i < args.Length; i++)
                {
                    if (args[i] is string)
                    {
                        args[i] = ((string)args[i]).Replace("[", "").Replace("]", "").Replace("{", "").Replace("}", "");
                    }
                }
                Console.WriteLine(string.Format(message, args));
            }
        }

        public static void DisplayErrorMessage(string action, string obj, string formattedID, string jsonmessage)
        {
            Console.WriteLine(string.Concat(Enumerable.Repeat("-", 100)));
            Octane.logger.Error(string.Concat(Enumerable.Repeat("-", 100)));
            Console.WriteLine("Could not {0} {1} [yellow]{2}[/]:", action, obj, formattedID);
            Octane.logger.Error($"Could not {action} {obj} [{formattedID}]:");
  
            try
            {
                JObject jsonObject = JObject.Parse(jsonmessage);

                // Create a new DataTable
                DataTable table = new DataTable();

                // Add columns to the DataTable
                table.Columns.Add("Property");
                table.Columns.Add("Value");

                // Check if the "errors" property exists
                if (jsonObject["errors"] != null)
                {
                    // Get the "errors" part of the JObject
                    JArray errors = (JArray)jsonObject["errors"];

                    // Iterate over each error in the errors array
                    foreach (JObject error in errors)
                    {
                        // Iterate over each property in the error JObject
                        foreach (JProperty property in error.Properties())
                        {
                            AddPropertyToDataTable(property, table);
                        }
                    }
                }
                else
                {
                    // Iterate over each property in the jsonObject
                    foreach (JProperty property in jsonObject.Properties())
                    {
                        AddPropertyToDataTable(property, table);
                    }
                }

                // Render the DataTable
                foreach (DataRow row in table.Rows)
                {
                    Console.WriteLine($"{row["Property"]}: {row["Value"]}");
                }

                Console.WriteLine(string.Concat(Enumerable.Repeat("-", 100)));
                Octane.logger.Error(string.Concat(Enumerable.Repeat("-", 100)));
            }
            catch (Exception) { Thread.Sleep(10000); }
        }

        private static void AddPropertyToDataTable(JProperty property, DataTable table)
        {
            table.Rows.Add(property.Name, property.Value);
        }

        // Connection string to the db Server and related database declacred in app.config
        private static string GetConnectionString(string dbserver, string database)
        {
            string ConnectionString = string.Empty;
            try
            {
                ConnectionString = @"Data Source=" + dbserver + ";Integrated Security=true;Initial Catalog=" + database + ";";
            }
            catch (Exception e)
            {
                DisplayMessage("An error occurred in get connection: {0}", e.Message);
                Octane.logger.Error(e, "An error occurred: {0}");
            }
            return ConnectionString;
        }

        public static DataTable GetDataTableFromSql(string dbserver, string database, string QueryRally)
        {
            var connectionString = GetConnectionString(dbserver, database);
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(QueryRally, conn))
                {
                    cmd.CommandTimeout = 0;
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        DataTable dataTable = new DataTable();
                        dataTable.Load(reader);
                        return dataTable;
                    }
                }
            }
        }

        public static HttpClient SignIn(string BaseUrl, string app)
        {
            HttpClientHandler handler = new HttpClientHandler()
            {
                Proxy = new WebProxy("nwbcproxy2.res.sys.shared.fortis:8080", false),
                UseProxy = true
            };
            HttpClient client = new HttpClient(handler);
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.Timeout = TimeSpan.FromSeconds(300);

            string connectionString = GetConnectionString(DbServer, Database); 
            string decryptedData = EncryptionService.ReadEncryptedDataFromDatabase(connectionString, app);
            string[] data = decryptedData.Split(';');
            ClientId = data[0];
            ClientSecret = data[1];
            WorkspaceId = data[2];
            SharedSpaceId = data[3];
            ParentWorkspaceId = data[4];

            var authData = new { client_id = ClientId, client_secret = ClientSecret };
            var content = new StringContent(JsonConvert.SerializeObject(authData), System.Text.Encoding.UTF8, "application/json");

            HttpResponseMessage response = client.PostAsync(BaseUrl + "authentication/sign_in", content).Result;

            if (response.IsSuccessStatusCode)
            {
                return client;
            }
            else
            {
                // Log the status code and reason for debugging purposes
                DisplayMessage("Status Code: {0}", (int)response.StatusCode);
                DisplayMessage("Reason: {0}", response.ReasonPhrase);
                Octane.logger.Error(response.ReasonPhrase, "An error occurred: {0}");
                DisplayMessage("Could not authenticate with Octane");
                return null;
            }
        }

        public static DataTable GetOctaneUsers(HttpClient client)
        {
            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("id", typeof(string));
            dataTable.Columns.Add("uid", typeof(string));
            dataTable.Columns.Add("email", typeof(string));
            dataTable.Columns.Add("type", typeof(string));
            dataTable.Columns.Add("roles", typeof(string));

            int totalCount = 0;
            int limit = 1000;
            int offset = 0;

            do
            {
                string usersUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/users?fields=id,uid,email&limit={limit}&offset={offset}";
                var response = client.GetAsync(usersUrl).Result;
                if (response.IsSuccessStatusCode)
                {
                    var json = response.Content.ReadAsStringAsync().Result;
                    JObject jObject = JObject.Parse(json);
                    JArray jArray = (JArray)jObject["data"];

                    foreach (JObject item in jArray)
                    {
                        DataRow row = dataTable.NewRow();
                        row["id"] = item["id"].ToString();
                        row["uid"] = item["uid"].ToString();
                        row["email"] = item["email"].ToString();
                        row["type"] = item["type"].ToString();
                        dataTable.Rows.Add(row);
                    }

                    totalCount = (int)jObject["total_count"];
                    offset += limit;
                }
                else
                {
                    break;
                }
            } while (offset < totalCount);

            return dataTable;
        }

        private static string searchUserId(DataTable dt, string EmailAddress, string FormattedID)
        {

            // Check if EmailAddress is null or empty
            if (string.IsNullOrEmpty(EmailAddress))
            {
                DisplayMessage("-");
                DisplayMessage("in Rally the EmailAddress is empty : {0} for [red]{1}[/] in Octane ", EmailAddress, FormattedID);
                DisplayMessage("-");
                Octane.logger.Error("in Rally the EmailAddress is empty : {0} for {1} in Octane ", EmailAddress, FormattedID);
                return string.Empty; // Return from the method
            }

            string result = string.Empty;
            try
            {
                var emailAddressPrefix = EmailAddress.Substring(0, EmailAddress.IndexOf('@'));

                result = (from row in dt.AsEnumerable()
                          let email = row.Field<string>("email")
                          let emailPrefix = email.Substring(0, email.IndexOf('@'))
                          where emailPrefix == emailAddressPrefix
                          select row.Field<string>("id")).FirstOrDefault();

                if (result == "" || result == null)
                {
                    DisplayMessage("-");
                    DisplayMessage("Cannot find the user {0} for [red]{1}[/] in Octane ", EmailAddress, FormattedID);
                    DisplayMessage("-");
                    Octane.logger.Error("Cannot find the user {0} for {1} in Octane ", EmailAddress, FormattedID);
                }
            }
            catch (Exception ex)
            {
                DisplayMessage("Error in searchUserId : {0} for the user : {1} for [red]{2}[/]", ex.Message, EmailAddress, FormattedID);
                Octane.logger.Error(ex.Message, EmailAddress, FormattedID, "Error in searchUserId : {0} for the user : {1} for {2}");
            }
            return result;
        }
        private static Tuple<string, string> ValueLookup(HttpClient client, string type, string rallyType, string portfolioItem)
        {
            string lookupValue = string.Empty;
            string lookupID = string.Empty;
            string search = string.Empty;
            string query = string.Empty;
            string lookupUrl = string.Empty;

            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("id", typeof(string));
            dataTable.Columns.Add("name", typeof(string));

            if (portfolioItem == "epic")
            {
                switch (type)
                {
                    case "phase":
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/phases?fields=id,name,entity&query=\"entity EQ ^epic^\"";
                        break;
                    case "epic type":
                        query = "query=\"(list_root={null})\"";
                        search = "text_search={\"type\":\"context\",\"text\":\"" + type + "\"}";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/list_nodes?fields=id,list_nodes,name&limit=10&{query}&{search}";
                        break;
                    default:
                        DisplayMessage("Invalid {0} type", type);
                        break;
                }
            }
            else
                if (portfolioItem == "feature")
            {
                switch (type)
                {
                    case "phase":
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/phases?fields=id,name,entity&query=\"entity EQ ^feature^\"";
                        break;
                    case "release":
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/releases?fields=id,name&limit=1500";
                        break;
                    case "feature type":
                        query = "query=\"(list_root={null})\"";
                        search = "text_search={\"type\":\"context\",\"text\":\"" + type + "\"}";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/list_nodes?fields=id,list_nodes,name&limit=10&{query}&{search}";
                        break;
                    case "wava test":
                        query = "query=\"(list_root={null})\"";
                        search = "text_search={\"type\":\"context\",\"text\":\"" + type + "\"}";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/list_nodes?fields=id,list_nodes,name&limit=10&{query}&{search}";
                        break;
                    case "perf test":
                        query = "query=\"(list_root={null})\"";
                        search = "text_search={\"type\":\"context\",\"text\":\"" + type + "\"}";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/list_nodes?fields=id,list_nodes,name&limit=10&{query}&{search}";
                        break;
                    case "test plan status":
                        query = "query=\"(list_root={null})\"";
                        search = "text_search={\"type\":\"context\",\"text\":\"" + type + "\"}";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/list_nodes?fields=id,list_nodes,name&limit=10&{query}&{search}";
                        break;
                    case "parent":
                        query = $"query=\"subtype EQ ^epic^ ;formatted_id_udf EQ ^{rallyType}^\"";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/work_items?fields=id,formatted_id_udf&limit=1&{query}";
                        break;
                    default:
                        DisplayMessage("Invalid {0} type", type);
                        break;
                }
            }
            else
            {
                // lookup userstory
                switch (type)
                {
                    case "phase":
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/phases?fields=id,name,entity&query=\"entity EQ ^story^\"";
                        break;
                    case "release":
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/releases?fields=id,name&query=\"name EQ ^{rallyType}^\"";
                        break;
                    case "sprint":
                        string var_release = rallyType != "" ? "202" + rallyType[3] + ".Q" + rallyType[6] : "";
                        string var_sprint = rallyType != "" ? "Sprint " + rallyType[10] : "";
                        string query_sprint = $"\"name EQ ^{var_sprint}^;release EQ {{name EQ ^{var_release}^}}\"";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/sprints?fields=id,name&query={query_sprint}";
                        break;
                    case "parent":
                        query = $"query=\"subtype EQ ^feature^ ;formatted_id_udf EQ ^{rallyType}^\"";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/work_items?fields=id,formatted_id_udf&limit=1&{query}";
                        break;
                    case "usparent":
                        query = $"query=\"subtype EQ ^story^ ;formatted_id_udf EQ ^{rallyType}^\"";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/work_items?fields=id,formatted_id_udf&limit=1&{query}";
                        break;
                    case "ready":
                        if (rallyType == "True")
                        {
                            return Tuple.Create(Ready_udf_List[0], "ready_udf");
                        }
                        else
                        {
                            return Tuple.Create(Ready_udf_List[1], "ready_udf");
                        }
                    case "team":
                        string rallyType_url = rallyType;
                        rallyType_url = rallyType_url.Replace("&", "%26");
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/teams?fields=id,name&query=\"name EQ ^{rallyType_url}^\"";
                        break;
                    default:
                        DisplayMessage("Invalid {0} type", type);
                        break;
                }
            }

            var response = client.GetAsync(lookupUrl).Result;

            if (response.IsSuccessStatusCode)
            {
                var json = response.Content.ReadAsStringAsync().Result;
                JObject jObject = JObject.Parse(json);
                JArray jArray = null;

                if (int.Parse(jObject["total_count"].ToString()) > 0)
                {

                    if (type == "phase" || type == "parent" || type == "release" || type == "sprint" || type == "team" || type == "release" || type == "usparent")
                    {
                        jArray = (JArray)jObject["data"];
                    }
                    else
                    {
                        jArray = (JArray)jObject["data"][0]["list_nodes"]["data"];
                    }

                    foreach (JObject item in jArray)
                    {
                        DataRow row = dataTable.NewRow();
                        row["id"] = item["id"].ToString();
                        if (type == "parent" || type == "usparent")
                        {
                            row["name"] = item["formatted_id_udf"].ToString();
                        }
                        else
                        {
                            row["name"] = item["name"].ToString();
                        }
                        dataTable.Rows.Add(row);
                    }
                    // look if in dataTable["name"] we find part of rallyType string. if so then return value of "name" in lookupValue
                    foreach (DataRow row in dataTable.Rows)
                    {
                        if (type == "wava test" || type == "perf test")
                        {
                            if (rallyType.ToString().Trim() == row["name"].ToString().Trim())
                            {
                                lookupValue = row["name"].ToString();
                                lookupID = row["id"].ToString();
                                break;
                            }
                        }
                        else
                        {
                            if (type == "sprint")
                            {
                                lookupValue = row["name"].ToString();
                                lookupID = row["id"].ToString();
                                break;
                            }
                            else
                            {
                                var var_rallyType = rallyType.ToLower();
                                var var_name = row["name"].ToString().ToLower();
                                if (var_rallyType.Contains(var_name))
                                {
                                    lookupValue = row["name"].ToString();
                                    lookupID = row["id"].ToString();
                                    break;
                                }
                            }
                        }
                    }
                }
                else
                {
                    DataRow row = dataTable.NewRow();
                    row["id"] = null;
                    row["name"] = null;
                    dataTable.Rows.Add(row);
                }
            }
            else
            {
                DisplayMessage("Request failed with status code: {0}", response.StatusCode);
                Octane.logger.Error(response.StatusCode.ToString(), "Request failed with status code");
            }
            return Tuple.Create(lookupID, lookupValue);
        }


        public static void HandleEpic(HttpClient client, DataTable dataTable)
        {
            Parallel.ForEach(dataTable.AsEnumerable(), row =>
            {
                if (row["StateName"].ToString() == null || row["StateName"].ToString() == "")
                {
                    DisplayMessage("The phase in Rally for the Epic {0} is empty : this is not allowed. Therefore [red]{0}[/] will not be added or updated.", row["formattedID"].ToString());
                    Octane.logger.Error("The phase in Rally for the Epic {0} is empty : this is not allowed. Therefore {0} will not be added or updated.", row["formattedID"].ToString());
                }
                else
                {
                    var epicName = row["formattedID"].ToString() + " " + row["name"].ToString();

                    var search_formattedid_epic_octane = "formatted_id_udf EQ ^" + row["formattedID"] + "^";
                    var urlEpic = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/epics?query=\"{search_formattedid_epic_octane}\"";
                    string search_epic = client.GetStringAsync(urlEpic).Result;

                    dynamic searchResult = JsonConvert.DeserializeObject<dynamic>(search_epic);
                    dynamic existingEpicArray = searchResult?.data as JArray;

                    var id_phase = ValueLookup(client, "phase", row["StateName"].ToString(), "epic").Item1;
                    var id_type = ValueLookup(client, "epic type", row["c_Type"].ToString(), "epic").Item1;
                    var id_user = searchUserId(dtUsers, row["EmailAddress"].ToString(), row["formattedID"].ToString());

                    if (existingEpicArray != null && existingEpicArray.Count > 0)
                    {
                        var epicData = new
                        {
                            name = epicName,
                            description = row["description"].ToString(),
                            formatted_id_udf = row["formattedid"].ToString(),
                            clarity_code_udf = row["c_ClarityID"].ToString(),
                            epic_type = string.IsNullOrEmpty(id_type) ? null : new
                            {
                                id = id_type,
                                type = "list_node",
                            },
                            ac_project_udf = row["TribeName"].ToString(),
                            phase = string.IsNullOrEmpty(id_phase) ? null : new
                            {
                                id = id_phase,
                                type = "phase",
                            },
                            owner = string.IsNullOrEmpty(id_user) ? null : new
                            {
                                id = id_user,
                                type = "workspace_user"
                            }
                        };
                        // Epic already exists, so update it using PUT request
                        var existingEpic = existingEpicArray[0];
                        var epicId = existingEpic.id;
                        var updateUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/epics/" + epicId;

                        var updateContent = new StringContent(JsonConvert.SerializeObject(epicData), System.Text.Encoding.UTF8, "application/json");

                        HttpResponseMessage updateResponse = client.PutAsync(updateUrl, updateContent).Result;

                        if (!updateResponse.IsSuccessStatusCode)
                        {
                            DisplayErrorMessage("update", "epic", row["formattedID"].ToString(), updateResponse.Content.ReadAsStringAsync().Result);
                        }
                        else
                        {
                            DisplayMessage("Epic updated: {0}", row["formattedid"].ToString());
                            Octane.logger.Info("Epic updated: {0} ", epicName);
                        }
                    }
                    else
                    {
                        var epicData = new
                        {
                            data = new[]
                            {
                            new
                            {
                                name = epicName,
                                description = row["description"].ToString(),
                                formatted_id_udf = row["formattedid"].ToString(),
                                clarity_code_udf = row["c_ClarityID"].ToString(),
                                ac_project_udf = row["TribeName"].ToString(),
                                epic_type = string.IsNullOrEmpty(id_type) ? null : new
                                {
                                  id = id_type,
                                  type = "list_node"
                                },
                                parent =  new
                                {
                                  id = "1002",
                                  type = "work_item_root"
                                },
                                phase = string.IsNullOrEmpty(id_phase) ? null : new
                                {
                                  id = id_phase,
                                  type = "phase",
                                },
                                owner = string.IsNullOrEmpty(id_user) ? null : new
                                {
                                  id = id_user,
                                  type = "workspace_user"
                                }
                            }
                        }
                        };
                        // Epic does not exist, so add it using POST request
                        var addUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/epics";

                        var addContent = new StringContent(JsonConvert.SerializeObject(epicData), System.Text.Encoding.UTF8, "application/json");

                        HttpResponseMessage addResponse = client.PostAsync(addUrl, addContent).Result;

                        if (!addResponse.IsSuccessStatusCode)
                        {
                            DisplayErrorMessage("add", "epic", row["formattedID"].ToString(), addResponse.Content.ReadAsStringAsync().Result);
                        }
                        else
                        {
                            DisplayMessage("Epic added: {0} ", row["formattedid"].ToString());
                            Octane.logger.Info("Epic added: {0} ", epicName);
                        }
                    }
                }
            });
        }
        public static string GetFeatureChildrenList(string FeatureID)
        {
            DataTable dtUS = GetDataTableFromSql(DbServer, Database, QueryRallyUserStory + " AND FE.FormattedID = '" + FeatureID + "'" + " AND US.LastUpdateDate >= '" + Octane.LastUpdateDate + "'");
            if (dtUS.Rows.Count == 0)
                return "No US";

            List<string> listUS = new List<string>();
            foreach (DataRow rw in dtUS.Rows)
            {
                listUS.Add(rw["USID"].ToString());
            }
            return string.Join(",", listUS);
        }

        public static void HandleFeature(HttpClient client, DataTable dataTable)
        {
            string formattedID = string.Empty;
            int counter = 0;
            try
            { 
                ParallelOptions options = new ParallelOptions();
                options.MaxDegreeOfParallelism = Environment.ProcessorCount * 2;

                Parallel.ForEach(dataTable.AsEnumerable(), options, (row, loopState) =>
                {
                    Interlocked.Increment(ref counter);
                    if (row["StateName"].ToString() == null || row["StateName"].ToString() == "")
                    {
                        string ListUSFeature = GetFeatureChildrenList(row["FeatureID"].ToString());
                        DisplayMessage("The phase in Rally for feature {0} is empty : this is not allowed. Therefore [red]{0}[/] will not be added or updated and its following children {1} too.", row["FeatureID"].ToString(), ListUSFeature);
                        Octane.logger.Error("The phase in Rally for feature {0} is empty : this is not allowed. Therefore {0} will not be added or updated and its following children {1} too.", row["FeatureID"].ToString(), ListUSFeature);
                    }
                    else
                    {
                        var featureName = row["FeatureID"].ToString() + " " + row["name"].ToString();
                        formattedID = row["FeatureID"].ToString();

                        var search_formattedid_feature_octane = "formatted_id_udf EQ ^" + row["FeatureID"] + "^";
                        var urlFeature = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/features?fields=trv_udf&query=\"{search_formattedid_feature_octane}\"";
                        string search_feature = client.GetStringAsync(urlFeature).Result;

                        dynamic searchResult = JsonConvert.DeserializeObject<dynamic>(search_feature);
                        dynamic existingFeatureArray = searchResult?.data as JArray;

                        var id_phase = ValueLookup(client, "phase", row["StateName"].ToString(), "feature").Item1;
                        var id_release = ValueLookup(client, "release", row["ReleaseName"].ToString(), "feature").Item1;
                        var id_type = ValueLookup(client, "feature type", row["c_Type"].ToString(), "feature").Item1;
                        var id_wafa = ValueLookup(client, "wava test", row["wafa_test"].ToString(), "feature").Item1;
                        var id_perf = ValueLookup(client, "perf test", row["perf_test"].ToString(), "feature").Item1;
                        var id_test_plan_status = ValueLookup(client, "test plan status", row["c_TestPlanStatus"].ToString(), "feature").Item1;
                        var id_parent = ValueLookup(client, "parent", row["OpusID"].ToString(), "feature").Item1;
                        var id_parent_type = "work_item";
                        DataTable dtMilestonesForFeatures = Helpers.GetDataTableFromSql(Helpers.DbServer, Helpers.Database, Helpers.QueryRallyMilestoneForFeature + " AND FE.FormattedID = '" + row["FeatureID"].ToString() + "'");
                        JArray milestones = GetMilestonesForFeature(client, dtMilestonesForFeatures, existingFeatureArray);
                        //JObject var_trv_udf = milestones.Count > 0 ? (JObject)milestones[0]["trv_udf"] : null;
                        JObject var_trv_udf = (JObject)milestones[0]["trv_udf"];


                        if (string.IsNullOrEmpty(id_release))
                        {
                            DisplayMessage("Feature : {0} -- The release {1} not found in Octane. action has to be taken.", row["FeatureID"].ToString(), row["ReleaseName"].ToString());
                            Octane.logger.Info("Feature : {0} -- The release {1} not found in Octane. action has to be taken.", row["FeatureID"].ToString(), row["ReleaseName"].ToString());
                        }

                        // if the parent (Opus) doesn't exist then it should be created before to add the feature
                        if (id_parent == null || id_parent == "")
                        {
                            if (string.IsNullOrEmpty(row["OpusID"].ToString()))
                            {
                                DataTable dtEPIC = GetDataTableFromSql(DbServer, Database, QueryRallyEpic + " WHERE FormattedID = '" + row["OpusID"].ToString() + "'");
                                DisplayMessage("The parent Epic {0} doesn't exist in Octane. It will be added before continuing with the feature {1}", row["OpusID"], row["FeatureID"].ToString());
                                Octane.logger.Info("The parent Epic {0} doesn't exist in Octane. It will be added before continuing with the feature {1}", row["OpusID"].ToString(), row["FeatureID"].ToString());
                                HandleEpic(client, dtEPIC);
                                id_parent = ValueLookup(client, "parent", row["OpusID"].ToString(), "feature").Item1;
                            }
                            else
                            {
                                id_parent = "1002";
                                id_parent_type = "work_item_root";
                            }
                        }

                        var id_user = searchUserId(dtUsers, row["EmailAddress"].ToString(), row["FeatureID"].ToString());

                        if (existingFeatureArray != null && existingFeatureArray.Count > 0)
                        {
                            var featureData = new
                            {
                                name = featureName,
                                description = row["description"].ToString(),
                                formatted_id_udf = row["featureid"].ToString(),
                                test_plan_remarks_udf = row["c_TestPlanRemarks"].ToString(),
                                business_wish_date_udf = row["c_Businesswishdate"].ToString(),
                                release_owner_udf = row["c_ReleaseOwner"].ToString(),
                                planned_start_date_udf = (row["PlannedStartDate"].ToString() == "01/01/1900 00:00:00") ? (DateTimeOffset?)null : new DateTimeOffset(DateTime.Parse(row["PlannedStartDate"].ToString())),
                                planned_end_date_udf = (row["PlannedEndDate"].ToString() == "01/01/1900 00:00:00") ? (DateTimeOffset?)null : new DateTimeOffset(DateTime.Parse(row["PlannedEndDate"].ToString())),

                                ac_project_udf = row["TribeName"].ToString(),

                                release = string.IsNullOrEmpty(id_release) ? null : new
                                {
                                    id = id_release,
                                    type = "release",
                                },

                                test_plan_status_udf = string.IsNullOrEmpty(id_test_plan_status) ? null : new
                                {
                                    id = id_test_plan_status,
                                    type = "list_node",
                                },

                                wava_test_udf = string.IsNullOrEmpty(id_wafa) ? null : new
                                {
                                    id = id_wafa,
                                    type = "list_node",
                                },

                                perf_test_udf = string.IsNullOrEmpty(id_perf) ? null : new
                                {
                                    id = id_perf,
                                    type = "list_node",
                                },

                                feature_type = string.IsNullOrEmpty(id_type) ? null : new
                                {
                                    id = id_type,
                                    type = "list_node",
                                },

                                phase = string.IsNullOrEmpty(id_phase) ? null : new
                                {
                                    id = id_phase,
                                    type = "phase",
                                },
                                owner = string.IsNullOrEmpty(id_user) ? null : new
                                {
                                    id = id_user,
                                    type = "workspace_user"
                                },
                                parent = string.IsNullOrEmpty(id_parent) ? null : new
                                {
                                    id = id_parent,
                                    type = id_parent_type
                                },
                                trv_udf = (var_trv_udf == null) ? null : new
                                {
                                    data = var_trv_udf["data"]
                                }
                            };

                            // Feature already exists, so update it using PUT request
                            var existingFeature = existingFeatureArray[0];
                            var featureId = existingFeature.id;
                            var updateUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/features/" + featureId;

                            var updateContent = new StringContent(JsonConvert.SerializeObject(featureData), System.Text.Encoding.UTF8, "application/json");

                            HttpResponseMessage updateResponse = client.PutAsync(updateUrl, updateContent).Result;
                            if (!updateResponse.IsSuccessStatusCode)
                            {
                                DisplayErrorMessage("update", "feature", row["FeatureID"].ToString(), updateResponse.Content.ReadAsStringAsync().Result);
                            }
                            else
                            {
                                DisplayMessage("Feature updated: {0} ", formattedID);
                                // search for all children (USs) and Update them as well
                                DataTable dtUS = GetDataTableFromSql(DbServer, Database, QueryRallyUserStory + " AND FE.FormattedID = '" + row["FeatureID"].ToString() + "'" + " AND US.LastUpdateDate >= '" + Octane.LastUpdateDate + "'"); //last
                                string listUS = string.Empty;
                                foreach (DataRow rw in dtUS.Rows)
                                {
                                    listUS = listUS + rw["USID"].ToString() + "/";
                                }
                                DisplayMessage("The USs {0} related to the feature {1} will be updated.", listUS, row["FeatureID"].ToString());
                                Octane.logger.Info("The USs {0} related to the feature {1} will be updated.", listUS, row["FeatureID"].ToString());

                                HandleStory(client, dtUS);
                                Octane.logger.Info("Feature updated with its children : {0} ", row["FeatureID"].ToString());
                            }
                        }
                        else
                        {
                            var featureData = new
                            {
                                data = new[]
                                {
                            new
                            {
                                name = featureName,
                                description = row["description"].ToString(),
                                formatted_id_udf = row["featureid"].ToString(),
                                business_wish_date_udf = row["c_Businesswishdate"].ToString(),
                                test_plan_remarks_udf = row["c_TestPlanRemarks"].ToString(),
                                release_owner_udf = row["c_ReleaseOwner"].ToString(),
                                planned_start_date_udf = (row["PlannedStartDate"].ToString() == "01/01/1900 00:00:00") ? (DateTimeOffset?)null : new DateTimeOffset(DateTime.Parse(row["PlannedStartDate"].ToString())),
                                planned_end_date_udf = (row["PlannedEndDate"].ToString() == "01/01/1900 00:00:00") ? (DateTimeOffset?)null : new DateTimeOffset(DateTime.Parse(row["PlannedEndDate"].ToString())),

                                ac_project_udf = row["TribeName"].ToString(),

                                release = string.IsNullOrEmpty(id_release) ? null : new
                                {
                                    id = id_release,
                                    type = "release",
                                },

                                test_plan_status_udf = string.IsNullOrEmpty(id_test_plan_status) ? null : new
                                {
                                    id = id_test_plan_status,
                                    type = "list_node",
                                },

                                wava_test_udf = string.IsNullOrEmpty(id_wafa) ? null : new
                                {
                                    id = id_wafa,
                                    type = "list_node",
                                },

                                perf_test_udf = string.IsNullOrEmpty(id_perf) ? null : new
                                {
                                    id = id_perf,
                                    type = "list_node",
                                },

                                feature_type = string.IsNullOrEmpty(id_type) ? null : new
                                {
                                    id = id_type,
                                    type = "list_node",
                                },

                                phase = string.IsNullOrEmpty(id_phase) ? null  : new
                                {
                                    id = id_phase,
                                    type = "phase",
                                },
                                owner = string.IsNullOrEmpty(id_user) ? null : new
                                { id = id_user,
                                  type = "workspace_user"
                                },
                                parent = string.IsNullOrEmpty(id_parent) ? null : new
                                {
                                  id = id_parent,
                                  type = id_parent_type
                                },
                                trv_udf = (var_trv_udf == null) ? null : new
                                {
                                    data = (var_trv_udf != null) ? var_trv_udf["data"] : null
                                }
                            }
                            }
                            };
                            // Feature does not exist, so add it using POST request
                            var addUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/features";

                            var addContent = new StringContent(JsonConvert.SerializeObject(featureData), System.Text.Encoding.UTF8, "application/json");

                            HttpResponseMessage addResponse = client.PostAsync(addUrl, addContent).Result;

                            if (!addResponse.IsSuccessStatusCode)
                            {
                                DisplayErrorMessage("add", "feature", row["FeatureID"].ToString(), addResponse.Content.ReadAsStringAsync().Result);
                            }
                            else
                            {
                                DisplayMessage("Feature added: {0}", formattedID);
                                // search for all children (USs) and add them as well
                                //DataTable dtUS = GetDataTableFromSql(DbServer, Database, QueryRallyUserStory + " AND FE.FormattedID = '" + row["FeatureID"].ToString() + "'" + Octane.QueryRallyStoryWhere);
                                //+ " AND US.LastUpdateDate >= '" + LastUpdateDate + "'"
                                //DataTable dtUS = GetDataTableFromSql(DbServer, Database, QueryRallyUserStory + " AND FE.FormattedID = '" + row["FeatureID"].ToString() + "'"); //last
                                DataTable dtUS = GetDataTableFromSql(DbServer, Database, QueryRallyUserStory + " AND FE.FormattedID = '" + row["FeatureID"].ToString() + "'" + " AND US.LastUpdateDate >= '" + Octane.LastUpdateDate + "'"); // Lastupdate for also USs
                                string listUS = string.Empty;
                                foreach (DataRow rw in dtUS.Rows)
                                {
                                    listUS = listUS + rw["USID"].ToString() + "/";
                                }
                                DisplayMessage("The USs {0} related to the feature {1} will be added.", listUS, row["FeatureID"].ToString());
                                Octane.logger.Info("The USs {0} related to the feature {1} will be added.", listUS, row["FeatureID"].ToString());

                                HandleStory(client, dtUS);
                                Octane.logger.Info("Feature added with its children : {0} ", row["FeatureID"].ToString());
                            }
                        }
                    }
                });
            }
            catch (HttpRequestException ex)
            {
                Helpers.DisplayMessage("Http Request failed in feature handling with status code: {0} - Feature : {1} - Current row handled : {2}", ex.Message, formattedID, counter);
                Octane.logger.Error(ex.Message, formattedID, counter, "Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}");
                Thread.Sleep(10000);
            }

            catch (Exception ex)
            {
                Helpers.DisplayMessage("A error in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}", ex.Message, formattedID, counter);
                Octane.logger.Error(ex.Message, formattedID, counter, "Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}");
            }
        }

        public static void HandleStory(HttpClient client, DataTable dataTable)
        {
            string formattedID = string.Empty;
            int counter = 0;
            try
            {
                ParallelOptions options = new ParallelOptions();
                options.MaxDegreeOfParallelism = Environment.ProcessorCount * 2;
                //foreach (DataRow row in dataTable.Rows)
                Parallel.ForEach(dataTable.AsEnumerable(), options, (row, loopState) =>
                {
                    Interlocked.Increment(ref counter);
                    if (row["ScheduleState"].ToString() == null || row["ScheduleState"].ToString() == "")
                    {
                        DisplayMessage("The phase in Rally for {0} is empty : this is not allowed. Therefore [red]{0}[/] will not be added or updated.", row["USID"].ToString());
                        Octane.logger.Error("The phase in Rally for {0} is empty : this is not allowed. Therefore {0} will not be added or updated.", row["USID"].ToString());
                    }
                    else
                    {
                        var storyName = row["USID"].ToString() + " " + row["name"].ToString();
                        formattedID = row["USID"].ToString();

                        var search_formattedid_story_octane = "formatted_id_udf EQ ^" + row["USID"] + "^";
                        var urlStory = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories?query=\"{search_formattedid_story_octane}\"";

                        string search_UserStory = string.Empty;
                        try
                        {
                            search_UserStory = client.GetStringAsync(urlStory).Result;
                        }
                        catch (HttpRequestException ex)
                        {
                            Helpers.DisplayMessage("Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}", ex.Message, formattedID, counter);
                            Octane.logger.Error(ex.Message, formattedID, counter, "Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}");
                            Thread.Sleep(10000);
                        }

                        dynamic searchResult = JsonConvert.DeserializeObject<dynamic>(search_UserStory);
                        dynamic existingStoryArray = searchResult?.data as JArray;

                        var id_phase = ValueLookup(client, "phase", row["ScheduleState"].ToString(), "userstory").Item1;
                        string var_sprint = row["sprint"].ToString();
                        string var_release = var_sprint != "" ? "202" + var_sprint[3] + ".Q" + var_sprint[6] : "";
                        var id_release = ValueLookup(client, "release", var_release, "userstory").Item1;
                        var id_sprint = ValueLookup(client, "sprint", row["sprint"].ToString(), "userstory").Item1;
                        var id_parent = ValueLookup(client, "parent", row["featureid"].ToString(), "userstory").Item1;
                        var id_parent_type = "work_item";
                        var id_usparent = ValueLookup(client, "usparent", row["usparentid"].ToString(), "userstory").Item1;
                        var id_ready = ValueLookup(client, "ready", row["Ready"].ToString(), "userstory").Item1;
                        var id_team = ValueLookup(client, "team", row["TribeName"].ToString(), "userstory").Item1;
                        if (string.IsNullOrEmpty(id_team))
                        {
                            DisplayMessage("US : {0} -- The team {1} not found in Octane. action has to be taken.", row["USID"].ToString(), row["TribeName"].ToString());
                            Octane.logger.Info("US : {0} -- The team {1} not found in Octane. action has to be taken.", row["USID"].ToString(), row["TribeName"].ToString());
                        }
                        if (string.IsNullOrEmpty(id_release))
                        {
                            DisplayMessage("US : {0} -- The release {1} not found in Octane. action has to be taken.", row["USID"].ToString(), var_release);
                            Octane.logger.Info("US : {0} -- The release {1} not found in Octane. action has to be taken.", row["USID"].ToString(), var_release);
                        }

                        // if both id_parent and id_usparent are null then we should check if featureid is not null or usprentid is not null
                        if (string.IsNullOrEmpty(id_parent) && string.IsNullOrEmpty(id_usparent))
                        {
                            // Case where FeatureID exist
                            if (!string.IsNullOrEmpty(row["FeatureID"].ToString()))
                            {
                                DataTable dtFEATURE = GetDataTableFromSql(DbServer, Database, QueryRallyFeature + " AND FE.FormattedID = '" + row["FeatureID"].ToString() + "'");
                                DisplayMessage("The parent Feature {0} doesn't exist in Octane. It will be added before continuing with the Story {1}", row["featureid"], formattedID);
                                Octane.logger.Info("The parentFeature {0} doesn't exist in Octane. It will be added before continuing with the Story {1}", row["featureid"].ToString(), formattedID);
                                HandleFeature(client, dtFEATURE);
                                id_parent = ValueLookup(client, "parent", row["FeatureID"].ToString(), "userstory").Item1;
                            }
                            // case where USPARENT exist
                            else if (!string.IsNullOrEmpty(row["USPARENTID"].ToString()))
                            {
                                DataTable dtSTORY = GetDataTableFromSql(DbServer, Database, QueryRallyUserStory + " AND US.formattedid = '" + row["USPARENTID"].ToString() + "'");
                                DisplayMessage("The parent Story {0} doesn't exist in Octane. It will be added before continuing with the Story {1}", row["USPARENTID"], formattedID);
                                Octane.logger.Info("The parent Story {0} doesn't exist in Octane. It will be added before continuing with the Story {1}", row["USPARENTID"].ToString(), formattedID);
                                HandleStory(client, dtSTORY);
                                id_parent = ValueLookup(client, "usparent", row["USPARENTID"].ToString(), "userstory").Item1;
                            }
                            else
                            {
                                id_parent = "1002";
                                id_parent_type = "work_item_root";
                            }
                        }
                        else if (string.IsNullOrEmpty(id_parent) && !string.IsNullOrEmpty(id_usparent))
                        {
                            id_parent = id_usparent;
                        }

                        var id_user = searchUserId(dtUsers, row["EmailAddress"].ToString(), row["USID"].ToString());


                        if (existingStoryArray != null && existingStoryArray.Count > 0)
                        {
                            var storyData = new
                            {
                                name = storyName,
                                description = row["description"].ToString(),
                                formatted_id_udf = row["USID"].ToString(),

                                story_points = (row["PlanEstimate"].ToString() == "0") ? (long?)null : Convert.ToInt64(Math.Round(Convert.ToDouble(row["PlanEstimate"].ToString()))),

                                ready_udf = string.IsNullOrEmpty(id_ready) ? null : new
                                {
                                    id = id_ready,
                                    type = "list_node",
                                },
                                team = string.IsNullOrEmpty(id_team) ? null : new
                                {
                                    id = id_team,
                                    type = "team",
                                },
                                release = string.IsNullOrEmpty(id_release) ? null : new
                                {
                                    id = id_release,
                                    type = "release",
                                },
                                sprint = string.IsNullOrEmpty(id_sprint) ? null : new
                                {
                                    id = id_sprint,
                                    type = "sprint",
                                },

                                phase = string.IsNullOrEmpty(id_phase) ? null : new
                                {
                                    id = id_phase,
                                    type = "phase",
                                },
                                owner = string.IsNullOrEmpty(id_user) ? null : new
                                {
                                    id = id_user,
                                    type = "workspace_user"
                                },
                                parent = string.IsNullOrEmpty(id_parent) ? null : new
                                {
                                    id = id_parent,
                                    type = id_parent_type
                                }
                            };

                            // Feature already exists, so update it using PUT request
                            var existingStory = existingStoryArray[0];
                            var storyId = existingStory.id;
                            var updateUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories/" + storyId;

                            var updateContent = new StringContent(JsonConvert.SerializeObject(storyData), System.Text.Encoding.UTF8, "application/json");

                            HttpResponseMessage updateResponse = client.PutAsync(updateUrl, updateContent).Result;

                            if (!updateResponse.IsSuccessStatusCode)
                            {
                                DisplayErrorMessage("update", "story", row["USID"].ToString(), updateResponse.Content.ReadAsStringAsync().Result);
                            }
                            else
                            {
                                DisplayMessage("Story updated: {0}", row["USID"].ToString());
                                Octane.logger.Info("Story updated: {0} ", row["USID"].ToString());
                            }
                        }
                        else
                        {
                            var storyData = new
                            {
                                data = new[]
                                {
                            new
                            {
                                name = storyName,
                                description = row["description"].ToString(),
                                formatted_id_udf = row["USID"].ToString(),

                                story_points = (row["PlanEstimate"].ToString() == "0") ? (long?)null : Convert.ToInt64(Math.Round(Convert.ToDouble(row["PlanEstimate"].ToString()))),

                                ready_udf = string.IsNullOrEmpty(id_ready) ? null : new
                                {
                                    id = id_ready,
                                    type = "list_node",
                                },
                                team = string.IsNullOrEmpty(id_team) ? null : new
                                {
                                    id = id_team,
                                    type = "team",
                                },
                                release = string.IsNullOrEmpty(id_release) ? null : new
                                {
                                    id = id_release,
                                    type = "release",
                                },
                                sprint = string.IsNullOrEmpty(id_sprint) ? null : new
                                {
                                    id = id_sprint,
                                    type = "sprint",
                                },
                                phase = string.IsNullOrEmpty(id_phase) ? null : new
                                {
                                    id = id_phase,
                                    type = "phase",
                                },
                                owner = string.IsNullOrEmpty(id_user) ? null : new
                                {
                                    id = id_user,
                                    type = "workspace_user"
                                },
                                parent = string.IsNullOrEmpty(id_parent) ? null : new
                                {
                                    id = id_parent,
                                    type = id_parent_type
                                }
                            }
                            }
                            };
                            // Story does not exist, so add it using POST request
                            var addUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories";

                            var addContent = new StringContent(JsonConvert.SerializeObject(storyData), System.Text.Encoding.UTF8, "application/json");

                            HttpResponseMessage addResponse = client.PostAsync(addUrl, addContent).Result;

                            if (!addResponse.IsSuccessStatusCode)
                            {
                                DisplayErrorMessage("add", "story", row["USID"].ToString(), addResponse.Content.ReadAsStringAsync().Result);
                            }
                            else
                            {
                                DisplayMessage("Story added: {0} ", row["USID"].ToString());
                                Octane.logger.Info("Story added: {0} ", row["USID"].ToString());
                            }
                        }
                    }
                });
            }
            catch (HttpRequestException ex)
            {
                Helpers.DisplayMessage("Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}", ex.Message, formattedID, counter);
                Octane.logger.Error(ex.Message, formattedID, counter, "Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}");
                Thread.Sleep(10000);
            }

            catch (Exception ex)
            {
                Helpers.DisplayMessage("A error in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}", ex.Message, formattedID, counter);
                Octane.logger.Error(ex.Message, formattedID, counter, "Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}");
            }
        }
        public static void HandleStory_SEQ(HttpClient client, DataTable dataTable)
        {
            string formattedID = string.Empty;
            int counter = 0;
            try
            {
                //ParallelOptions options = new ParallelOptions();
                //options.MaxDegreeOfParallelism = Environment.ProcessorCount * 2;
                foreach (DataRow row in dataTable.Rows)
                //Parallel.ForEach(dataTable.AsEnumerable(), options, (row, loopState) =>
                {
                    //Interlocked.Increment(ref counter);
                    if (row["ScheduleState"].ToString() == null || row["ScheduleState"].ToString() == "")
                    {
                        DisplayMessage("The phase in Rally for {0} is empty : this is not allowed. Therefore [red]{0}[/] will not be added or updated.", row["USID"].ToString());
                        Octane.logger.Error("The phase in Rally for {0} is empty : this is not allowed. Therefore {0} will not be added or updated.", row["USID"].ToString());
                    }
                    else
                    {
                        var storyName = row["USID"].ToString() + " " + row["name"].ToString();
                        formattedID = row["USID"].ToString();

                        var search_formattedid_story_octane = "formatted_id_udf EQ ^" + row["USID"] + "^";
                        var urlStory = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories?query=\"{search_formattedid_story_octane}\"";

                        string search_UserStory = string.Empty;
                        try
                        {
                            search_UserStory = client.GetStringAsync(urlStory).Result;
                        }
                        catch (HttpRequestException ex)
                        {
                            Helpers.DisplayMessage("Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}", ex.Message, formattedID, counter);
                            Octane.logger.Error(ex.Message, formattedID, counter, "Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}");
                            Thread.Sleep(10000);
                        }

                        dynamic searchResult = JsonConvert.DeserializeObject<dynamic>(search_UserStory);
                        dynamic existingStoryArray = searchResult?.data as JArray;

                        var id_phase = ValueLookup(client, "phase", row["ScheduleState"].ToString(), "userstory").Item1;
                        string var_sprint = row["sprint"].ToString();
                        string var_release = var_sprint != "" ? "202" + var_sprint[3] + ".Q" + var_sprint[6] : "";
                        var id_release = ValueLookup(client, "release", var_release, "userstory").Item1;
                        var id_sprint = ValueLookup(client, "sprint", row["sprint"].ToString(), "userstory").Item1;
                        var id_parent = ValueLookup(client, "parent", row["featureid"].ToString(), "userstory").Item1;
                        var id_parent_type = "work_item";
                        var id_usparent = ValueLookup(client, "usparent", row["usparentid"].ToString(), "userstory").Item1;
                        var id_ready = ValueLookup(client, "ready", row["Ready"].ToString(), "userstory").Item1;
                        var id_team = ValueLookup(client, "team", row["TribeName"].ToString(), "userstory").Item1;
                        if (string.IsNullOrEmpty(id_team))
                        {
                            DisplayMessage("US : {0} -- The team {1} not found in Octane. action has to be taken.", row["USID"].ToString(), row["TribeName"].ToString());
                            Octane.logger.Info("US : {0} -- The team {1} not found in Octane. action has to be taken.", row["USID"].ToString(), row["TribeName"].ToString());
                        }
                        if (string.IsNullOrEmpty(id_release))
                        {
                            DisplayMessage("US : {0} -- The release {1} not found in Octane. action has to be taken.", row["USID"].ToString(), var_release);
                            Octane.logger.Info("US : {0} -- The release {1} not found in Octane. action has to be taken.", row["USID"].ToString(), var_release);
                        }

                        // if both id_parent and id_usparent are null then we should check if featureid is not null or usprentid is not null
                        if (string.IsNullOrEmpty(id_parent) && string.IsNullOrEmpty(id_usparent))
                        {
                            // Case where FeatureID exist
                            if (!string.IsNullOrEmpty(row["FeatureID"].ToString()))
                            {
                                DataTable dtFEATURE = GetDataTableFromSql(DbServer, Database, QueryRallyFeature + " AND FE.FormattedID = '" + row["FeatureID"].ToString() + "'");
                                DisplayMessage("The parent Feature {0} doesn't exist in Octane. It will be added before continuing with the Story {1}", row["featureid"], formattedID);
                                Octane.logger.Info("The parentFeature {0} doesn't exist in Octane. It will be added before continuing with the Story {1}", row["featureid"].ToString(), formattedID);
                                HandleFeature(client, dtFEATURE);
                                id_parent = ValueLookup(client, "parent", row["FeatureID"].ToString(), "userstory").Item1;
                            }
                            // case where USPARENT exist
                            else if (!string.IsNullOrEmpty(row["USPARENTID"].ToString()))
                            {
                                DataTable dtSTORY = GetDataTableFromSql(DbServer, Database, QueryRallyUserStory + " AND US.formattedid = '" + row["USPARENTID"].ToString() + "'");
                                DisplayMessage("The parent Story {0} doesn't exist in Octane. It will be added before continuing with the Story {1}", row["USPARENTID"], formattedID);
                                Octane.logger.Info("The parent Story {0} doesn't exist in Octane. It will be added before continuing with the Story {1}", row["USPARENTID"].ToString(), formattedID);
                                HandleStory(client, dtSTORY);
                                id_parent = ValueLookup(client, "usparent", row["USPARENTID"].ToString(), "userstory").Item1;
                            }
                            else
                            {
                                id_parent = "1002";
                                id_parent_type = "work_item_root";
                            }
                        }
                        else if (string.IsNullOrEmpty(id_parent) && !string.IsNullOrEmpty(id_usparent))
                        {
                            id_parent = id_usparent;
                        }

                        var id_user = searchUserId(dtUsers, row["EmailAddress"].ToString(), row["USID"].ToString());


                        if (existingStoryArray != null && existingStoryArray.Count > 0)
                        {
                            var storyData = new
                            {
                                name = storyName,
                                description = row["description"].ToString(),
                                formatted_id_udf = row["USID"].ToString(),

                                story_points = (row["PlanEstimate"].ToString() == "0") ? (long?)null : Convert.ToInt64(Math.Round(Convert.ToDouble(row["PlanEstimate"].ToString()))),

                                ready_udf = string.IsNullOrEmpty(id_ready) ? null : new
                                {
                                    id = id_ready,
                                    type = "list_node",
                                },
                                team = string.IsNullOrEmpty(id_team) ? null : new
                                {
                                    id = id_team,
                                    type = "team",
                                },
                                release = string.IsNullOrEmpty(id_release) ? null : new
                                {
                                    id = id_release,
                                    type = "release",
                                },
                                sprint = string.IsNullOrEmpty(id_sprint) ? null : new
                                {
                                    id = id_sprint,
                                    type = "sprint",
                                },

                                phase = string.IsNullOrEmpty(id_phase) ? null : new
                                {
                                    id = id_phase,
                                    type = "phase",
                                },
                                owner = string.IsNullOrEmpty(id_user) ? null : new
                                {
                                    id = id_user,
                                    type = "workspace_user"
                                },
                                parent = string.IsNullOrEmpty(id_parent) ? null : new
                                {
                                    id = id_parent,
                                    type = id_parent_type
                                }
                            };

                            // Feature already exists, so update it using PUT request
                            var existingStory = existingStoryArray[0];
                            var storyId = existingStory.id;
                            var updateUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories/" + storyId;

                            var updateContent = new StringContent(JsonConvert.SerializeObject(storyData), System.Text.Encoding.UTF8, "application/json");

                            HttpResponseMessage updateResponse = client.PutAsync(updateUrl, updateContent).Result;

                            if (!updateResponse.IsSuccessStatusCode)
                            {
                                DisplayErrorMessage("update", "story", row["USID"].ToString(), updateResponse.Content.ReadAsStringAsync().Result);
                            }
                            else
                            {
                                DisplayMessage("Story updated: {0}", row["USID"].ToString());
                                Octane.logger.Info("Story updated: {0} ", row["USID"].ToString());
                            }
                        }
                        else
                        {
                            var storyData = new
                            {
                                data = new[]
                                {
                            new
                            {
                                name = storyName,
                                description = row["description"].ToString(),
                                formatted_id_udf = row["USID"].ToString(),

                                story_points = (row["PlanEstimate"].ToString() == "0") ? (long?)null : Convert.ToInt64(Math.Round(Convert.ToDouble(row["PlanEstimate"].ToString()))),

                                ready_udf = string.IsNullOrEmpty(id_ready) ? null : new
                                {
                                    id = id_ready,
                                    type = "list_node",
                                },
                                team = string.IsNullOrEmpty(id_team) ? null : new
                                {
                                    id = id_team,
                                    type = "team",
                                },
                                release = string.IsNullOrEmpty(id_release) ? null : new
                                {
                                    id = id_release,
                                    type = "release",
                                },
                                sprint = string.IsNullOrEmpty(id_sprint) ? null : new
                                {
                                    id = id_sprint,
                                    type = "sprint",
                                },
                                phase = string.IsNullOrEmpty(id_phase) ? null : new
                                {
                                    id = id_phase,
                                    type = "phase",
                                },
                                owner = string.IsNullOrEmpty(id_user) ? null : new
                                {
                                    id = id_user,
                                    type = "workspace_user"
                                },
                                parent = string.IsNullOrEmpty(id_parent) ? null : new
                                {
                                    id = id_parent,
                                    type = id_parent_type
                                }
                            }
                            }
                            };
                            // Story does not exist, so add it using POST request
                            var addUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories";

                            var addContent = new StringContent(JsonConvert.SerializeObject(storyData), System.Text.Encoding.UTF8, "application/json");

                            HttpResponseMessage addResponse = client.PostAsync(addUrl, addContent).Result;

                            if (!addResponse.IsSuccessStatusCode)
                            {
                                DisplayErrorMessage("add", "story", row["USID"].ToString(), addResponse.Content.ReadAsStringAsync().Result);
                            }
                            else
                            {
                                DisplayMessage("Story added: {0} ", row["USID"].ToString());
                                Octane.logger.Info("Story added: {0} ", row["USID"].ToString());
                            }
                        }
                    }
                }//
            }
            catch (HttpRequestException ex)
            {
                Helpers.DisplayMessage("Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}", ex.Message, formattedID, counter);
                Octane.logger.Error(ex.Message, formattedID, counter, "Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}");
                Thread.Sleep(10000);
            }

            catch (Exception ex)
            {
                Helpers.DisplayMessage("A error in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}", ex.Message, formattedID, counter);
                Octane.logger.Error(ex.Message, formattedID, counter, "Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}");
            }
        }

        public static void HandleStory_No_Parent(HttpClient client, DataTable dataTable)
        {
            string formattedID = string.Empty;
            int counter = 0;
            try
            {
                ParallelOptions options = new ParallelOptions();
                options.MaxDegreeOfParallelism = Environment.ProcessorCount * 2;
                //foreach (DataRow row in dataTable.Rows)
                Parallel.ForEach(dataTable.AsEnumerable(), options, (row, loopState) =>
                {
                    Interlocked.Increment(ref counter);
                    if (row["ScheduleState"].ToString() == null || row["ScheduleState"].ToString() == "")
                    {
                        DisplayMessage("The phase in Rally for {0} is empty : this is not allowed. Therefore [red]{0}[/] will not be added or updated.", row["USID"].ToString());
                        Octane.logger.Error("The phase in Rally for {0} is empty : this is not allowed. Therefore {0} will not be added or updated.", row["USID"].ToString());
                    }
                    else
                    {
                        var storyName = row["USID"].ToString() + " " + row["name"].ToString();
                        formattedID = row["USID"].ToString();

                        var search_formattedid_story_octane = "formatted_id_udf EQ ^" + row["USID"] + "^";
                        var urlStory = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories?query=\"{search_formattedid_story_octane}\"";

                        string search_UserStory = string.Empty;
                        try
                        {
                            search_UserStory = client.GetStringAsync(urlStory).Result;
                        }
                        catch (HttpRequestException ex)
                        {
                            Helpers.DisplayMessage("Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}", ex.Message, formattedID, counter);
                            Octane.logger.Error(ex.Message, formattedID, counter, "Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}");
                            Thread.Sleep(10000);
                        }

                        dynamic searchResult = JsonConvert.DeserializeObject<dynamic>(search_UserStory);
                        dynamic existingStoryArray = searchResult?.data as JArray;

                        var id_phase = ValueLookup(client, "phase", row["ScheduleState"].ToString(), "userstory").Item1;
                        string var_sprint = row["sprint"].ToString();
                        string var_release = var_sprint != "" ? "202" + var_sprint[3] + ".Q" + var_sprint[6] : "";
                        var id_release = ValueLookup(client, "release", var_release, "userstory").Item1;
                        var id_sprint = ValueLookup(client, "sprint", row["sprint"].ToString(), "userstory").Item1;
                        var id_parent = ValueLookup(client, "parent", row["featureid"].ToString(), "userstory").Item1;
                        var id_parent_type = "work_item";
                        var id_usparent = ValueLookup(client, "usparent", row["usparentid"].ToString(), "userstory").Item1;
                        var id_ready = ValueLookup(client, "ready", row["Ready"].ToString(), "userstory").Item1;
                        var id_team = ValueLookup(client, "team", row["TribeName"].ToString(), "userstory").Item1;
                        if (string.IsNullOrEmpty(id_team))
                        {
                            DisplayMessage("US : {0} -- The team {1} not found in Octane. action has to be taken.", row["USID"].ToString(), row["TribeName"].ToString());
                            Octane.logger.Info("US : {0} -- The team {1} not found in Octane. action has to be taken.", row["USID"].ToString(), row["TribeName"].ToString());
                        }
                        if (string.IsNullOrEmpty(id_release))
                        {
                            DisplayMessage("US : {0} -- The release {1} not found in Octane. action has to be taken.", row["USID"].ToString(), var_release);
                            Octane.logger.Info("US : {0} -- The release {1} not found in Octane. action has to be taken.", row["USID"].ToString(), var_release);
                        }

                        if (!string.IsNullOrEmpty(id_usparent))
                        {
                            id_parent = id_usparent;
                        }
                        else
                        {
                            id_parent = "1002";
                            id_parent_type = "work_item_root";
                        }

                        var id_user = searchUserId(dtUsers, row["EmailAddress"].ToString(), row["USID"].ToString());


                        if (existingStoryArray != null && existingStoryArray.Count > 0)
                        {
                            var storyData = new
                            {
                                name = storyName,
                                description = row["description"].ToString(),
                                formatted_id_udf = row["USID"].ToString(),

                                story_points = (row["PlanEstimate"].ToString() == "0") ? (long?)null : Convert.ToInt64(Math.Round(Convert.ToDouble(row["PlanEstimate"].ToString()))),

                                ready_udf = string.IsNullOrEmpty(id_ready) ? null : new
                                {
                                    id = id_ready,
                                    type = "list_node",
                                },
                                team = string.IsNullOrEmpty(id_team) ? null : new
                                {
                                    id = id_team,
                                    type = "team",
                                },
                                release = string.IsNullOrEmpty(id_release) ? null : new
                                {
                                    id = id_release,
                                    type = "release",
                                },
                                sprint = string.IsNullOrEmpty(id_sprint) ? null : new
                                {
                                    id = id_sprint,
                                    type = "sprint",
                                },

                                phase = string.IsNullOrEmpty(id_phase) ? null : new
                                {
                                    id = id_phase,
                                    type = "phase",
                                },
                                owner = string.IsNullOrEmpty(id_user) ? null : new
                                {
                                    id = id_user,
                                    type = "workspace_user"
                                },
                                parent = string.IsNullOrEmpty(id_parent) ? null : new
                                {
                                    id = id_parent,
                                    type = id_parent_type
                                }
                            };

                            // Feature already exists, so update it using PUT request
                            var existingStory = existingStoryArray[0];
                            var storyId = existingStory.id;
                            var updateUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories/" + storyId;

                            var updateContent = new StringContent(JsonConvert.SerializeObject(storyData), System.Text.Encoding.UTF8, "application/json");

                            HttpResponseMessage updateResponse = client.PutAsync(updateUrl, updateContent).Result;

                            if (!updateResponse.IsSuccessStatusCode)
                            {
                                DisplayErrorMessage("update", "story", row["USID"].ToString(), updateResponse.Content.ReadAsStringAsync().Result);
                            }
                            else
                            {
                                DisplayMessage("Story updated: {0}", row["USID"].ToString());
                                Octane.logger.Info("Story updated: {0} ", row["USID"].ToString());
                            }
                        }
                        else
                        {
                            var storyData = new
                            {
                                data = new[]
                                {
                            new
                            {
                                name = storyName,
                                description = row["description"].ToString(),
                                formatted_id_udf = row["USID"].ToString(),

                                story_points = (row["PlanEstimate"].ToString() == "0") ? (long?)null : Convert.ToInt64(Math.Round(Convert.ToDouble(row["PlanEstimate"].ToString()))),

                                ready_udf = string.IsNullOrEmpty(id_ready) ? null : new
                                {
                                    id = id_ready,
                                    type = "list_node",
                                },
                                team = string.IsNullOrEmpty(id_team) ? null : new
                                {
                                    id = id_team,
                                    type = "team",
                                },
                                release = string.IsNullOrEmpty(id_release) ? null : new
                                {
                                    id = id_release,
                                    type = "release",
                                },
                                sprint = string.IsNullOrEmpty(id_sprint) ? null : new
                                {
                                    id = id_sprint,
                                    type = "sprint",
                                },
                                phase = string.IsNullOrEmpty(id_phase) ? null : new
                                {
                                    id = id_phase,
                                    type = "phase",
                                },
                                owner = string.IsNullOrEmpty(id_user) ? null : new
                                {
                                    id = id_user,
                                    type = "workspace_user"
                                },
                                parent = string.IsNullOrEmpty(id_parent) ? null : new
                                {
                                    id = id_parent,
                                    type = id_parent_type
                                }
                            }
                            }
                            };
                            // Story does not exist, so add it using POST request
                            var addUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories";

                            var addContent = new StringContent(JsonConvert.SerializeObject(storyData), System.Text.Encoding.UTF8, "application/json");

                            HttpResponseMessage addResponse = client.PostAsync(addUrl, addContent).Result;

                            if (!addResponse.IsSuccessStatusCode)
                            {
                                DisplayErrorMessage("add", "story", row["USID"].ToString(), addResponse.Content.ReadAsStringAsync().Result);
                            }
                            else
                            {
                                DisplayMessage("Story added: {0} ", row["USID"].ToString());
                                Octane.logger.Info("Story added: {0} ", row["USID"].ToString());
                            }
                        }
                    }
                });
            }
            catch (HttpRequestException ex)
            {
                Helpers.DisplayMessage("Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}", ex.Message, formattedID, counter);
                Octane.logger.Error(ex.Message, formattedID, counter, "Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}");
                Thread.Sleep(10000);
            }

            catch (Exception ex)
            {
                Helpers.DisplayMessage("A error in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}", ex.Message, formattedID, counter);
                Octane.logger.Error(ex.Message, formattedID, counter, "Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}");
            }
        }
        public static DataTable GetMilestones(HttpClient client)
        {
            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("id", typeof(string));
            dataTable.Columns.Add("name", typeof(string));
            dataTable.Columns.Add("date", typeof(DateTime));

            int totalCount = 0;
            int limit = 1000;
            int offset = 0;

            do
            {
                string usersUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/milestones?fields=id,name,date&limit={limit}&offset={offset}";
                var response = client.GetAsync(usersUrl).Result;
                if (response.IsSuccessStatusCode)
                {
                    var json = response.Content.ReadAsStringAsync().Result;
                    JObject jObject = JObject.Parse(json);
                    JArray jArray = (JArray)jObject["data"];

                    foreach (JObject item in jArray)
                    {
                        DataRow row = dataTable.NewRow();
                        row["id"] = item["id"].ToString();
                        row["name"] = item["name"].ToString();
                        row["date"] = item["date"].ToString();
                        dataTable.Rows.Add(row);
                    }

                    totalCount = (int)jObject["total_count"];
                    offset += limit;
                }
                else
                {
                    break;
                }
            } while (offset < totalCount);

            return dataTable;
        }

        public static JArray GetMilestonesForFeature(HttpClient client, DataTable dtMilestonesForFeatures, JArray featureArray)
        {
            dtMilestonesForFeatures.Columns.Add("milestone_id_octane", typeof(int));

            foreach (DataRow row in dtMilestonesForFeatures.Rows)
            {
                string milestoneid = row[colMilstone].ToString();
                string usersUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/milestones?fields=id&query=\"formattedid_udf EQ ^{milestoneid}^\"";

                var response = client.GetAsync(usersUrl).Result;
                var data = response.Content.ReadAsStringAsync().Result;
                var milestoneData = JsonConvert.DeserializeObject<dynamic>(data);

                row["milestone_id_octane"] = milestoneData.total_count > 0 ? int.Parse(milestoneData.data[0].id.ToString()) : 0;
            }

            if (featureArray == null || featureArray.Count == 0)
            {
                featureArray = new JArray();
                JObject feature = new JObject
                {
                    ["type"] = "feature",
                    ["id"] = "", // replace with your feature id
                    ["logical_name"] = "", // replace with your logical name
                    ["workspace_id"] = 1003, // replace with your workspace id
                    ["trv_udf"] = new JObject
                    {
                        ["total_count"] = 0,
                        ["data"] = new JArray()
                    }
                };

                foreach (DataRow row in dtMilestonesForFeatures.Rows)
                {
                    JObject newMilestone = new JObject
                    {
                        ["type"] = "milestone",
                        ["id"] = row["milestone_id_octane"].ToString(),
                        ["activity_level"] = 0,
                        ["name"] = row["name"].ToString()
                    };
                    ((JArray)feature["trv_udf"]["data"]).Add(newMilestone);
                    DisplayMessage(message_milestone, row[colMilstone].ToString() + " - " + row["name"].ToString(), row["featureid"].ToString());
                    Octane.logger.Info(message_milestone, row["milestoneid"].ToString() + " - " + row["name"].ToString(), row["featureid"].ToString());
                }

                featureArray.Add(feature);
            }

            else
            {
                foreach (JObject feature in featureArray)
                {
                    if (feature["trv_udf"] != null)
                    {
                        feature["trv_udf"]["data"] = new JArray(); // Replace the old JArray with a new, empty one

                        foreach (DataRow row in dtMilestonesForFeatures.Rows)
                        {
                            JObject newMilestone = new JObject
                            {
                                ["type"] = "milestone",
                                ["id"] = row["milestone_id_octane"].ToString(),
                                ["activity_level"] = 0,
                                ["name"] = row["name"].ToString()
                            };
                            ((JArray)feature["trv_udf"]["data"]).Add(newMilestone);
                            DisplayMessage(message_milestone, row[colMilstone].ToString() + " - " + row["name"].ToString(), row["featureid"].ToString());
                            Octane.logger.Info(message_milestone, row["milestoneid"].ToString() + " - " + row["name"].ToString(), row["featureid"].ToString());
                        }
                    }
                    else
                    {
                        feature["trv_udf"] = new JObject
                        {
                            ["total_count"] = 0,
                            ["data"] = new JArray()
                        };
                    }
                }
            }

            return featureArray;
        }

        public static void HandleMilestone(HttpClient client, DataTable dataTable)
        {
            
            foreach (DataRow row in dataTable.Rows)
            {

                var milestoneid = row[colMilstone].ToString();
                var milestoneName = row["name"].ToString();
                var search_milestone_octane = "formattedid_udf EQ ^" + row[colMilstone] + "^";
                var urlMilestone = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/milestones?query=\"{search_milestone_octane}\"";
                string search_milestone = client.GetStringAsync(urlMilestone).Result;

                dynamic searchResult = JsonConvert.DeserializeObject<dynamic>(search_milestone);
                dynamic existingMilestoneArray = searchResult?.data as JArray;

                if (existingMilestoneArray != null && existingMilestoneArray.Count > 0)
                {
                    var milestoneData = new
                    {
                        formattedid_udf = milestoneid,
                        name = milestoneName,
                        date = (row["TargetDate"].ToString() == "01/01/1900 00:00:00") ? (DateTimeOffset?)null : new DateTimeOffset(DateTime.Parse(row["TargetDate"].ToString())),
                    };
                    // Epic already exists, so update it using PUT request
                    var existingMilestone = existingMilestoneArray[0];
                    var milestoneId = existingMilestone.id;
                    var updateUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/milestones/" + milestoneId;

                    var updateContent = new StringContent(JsonConvert.SerializeObject(milestoneData), System.Text.Encoding.UTF8, "application/json");

                    HttpResponseMessage updateResponse = client.PutAsync(updateUrl, updateContent).Result;

                    if (!updateResponse.IsSuccessStatusCode)
                    {
                        DisplayErrorMessage("update", "milestone", milestoneid + " - " + row["name"].ToString(), updateResponse.Content.ReadAsStringAsync().Result);
                    }
                    else
                    {
                        DisplayMessage($"Milestone updated: {milestoneid} - {milestoneName}");
                        Octane.logger.Info($"Milestone updated: {milestoneid} - {milestoneName}");
                    }
                }
                else
                {
                    var milestoneData = new
                    {
                        data = new[]
                        {
                                new
                                {
                                  formattedid_udf = milestoneid,
                                  name = milestoneName,
                                  date = (row["TargetDate"].ToString() == "01/01/1900 00:00:00") ? (DateTimeOffset?)null : new DateTimeOffset(DateTime.Parse(row["TargetDate"].ToString())),
                                }
                            }
                    };
                    // Epic does not exist, so add it using POST request
                    var addUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/milestones";

                    var addContent = new StringContent(JsonConvert.SerializeObject(milestoneData), System.Text.Encoding.UTF8, "application/json");

                    HttpResponseMessage addResponse = client.PostAsync(addUrl, addContent).Result;

                    if (!addResponse.IsSuccessStatusCode)
                    {
                        DisplayErrorMessage("add", "milestone", milestoneid + " - " + row["name"].ToString(), addResponse.Content.ReadAsStringAsync().Result);
                    }
                    else
                    {
                        DisplayMessage($"Milestone added: {milestoneid} - {milestoneName}");
                        Octane.logger.Info($"Milestone added: {milestoneid} - {milestoneName}");
                    }
                }
            } 
        }

        public static string GetFormattedId(string milestoneName, string connectionString)
        {
            string formattedId = string.Empty;

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string query = ReadConfiguration(ConfigTable, "query", "GetMilestone");
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@milestoneName", milestoneName);

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            formattedId = reader["formattedid"].ToString();
                        }
                    }
                }
            }

            return formattedId;
        }

        public static void HandleDuplicateMilestone(HttpClient client, DataTable dataTable)
        {
            string connectionString = GetConnectionString(DbServer, Database);
            

            foreach (DataRow row in dataTable.Rows)
            //Parallel.ForEach(dataTable.AsEnumerable(), row =>
            {
                var milestoneid = row[colMilstone].ToString();
                var milestoneName = row["milestonename"].ToString();
                var milestoneFormattedid = GetFormattedId(milestoneName, connectionString);
                var search_id_milestone_octane = row[colMilstone].ToString();
                var urlMilestone = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/milestones/{search_id_milestone_octane}";
                string search_milestone = client.GetStringAsync(urlMilestone).Result;

                dynamic searchResult = JsonConvert.DeserializeObject<dynamic>(search_milestone);
                dynamic existingMilestone = searchResult as JObject;

                if (existingMilestone != null)
                {
                    var milestoneData = new
                    {
                        formattedid_udf = milestoneFormattedid,
                    };
                    

                    var milestoneId = existingMilestone.id;
                    var updateUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/milestones/" + milestoneId;

                    var updateContent = new StringContent(JsonConvert.SerializeObject(milestoneData), System.Text.Encoding.UTF8, "application/json");

                    HttpResponseMessage updateResponse = client.PutAsync(updateUrl, updateContent).Result;

                    if (!updateResponse.IsSuccessStatusCode)
                    {
                        DisplayErrorMessage("update", "milestone", milestoneid + " - " + row["name"].ToString(), updateResponse.Content.ReadAsStringAsync().Result);
                    }
                    else
                    {
                        DisplayMessage($"Duplicate Milestone - name updated: {milestoneid} - {milestoneName}");
                        Octane.logger.Info($"Duplicate Milestone - name updated: {milestoneid} - {milestoneName}");
                    }
                }
                else
                {
                    DisplayMessage($"Duplicate Milestone not found: {milestoneid} - {milestoneName}");
                    Octane.logger.Info($"Duplicate Milestone not found: {milestoneid} - {milestoneName}");
                }
            }
        }

    }
}
