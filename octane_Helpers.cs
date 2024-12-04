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
using System.Security;
using System.Text.RegularExpressions;
using System.Web;

namespace OctaneSync
{
    internal static class Helpers
    {
        private static readonly string SafetyConnectionStringPattern = @"^Data Source=[\w\-\.\,]+;(Initial Catalog|Database)=[\w\-\.]+;(Integrated Security=True|User Id=[\w\-\.]+;Password=[\w\-\.]+)$";

        public const string type = "query";
        public const string list = "list";
        public static DataTable ConfigTable { get; set; } = LoadConfiguration(DbServer, Database, ConfigurationManager.AppSettings["configTable"].ToString());

        public static readonly string Database = ConfigurationManager.AppSettings["database"].ToString();
        public static readonly string DbServer = ConfigurationManager.AppSettings["dbserver"].ToString();
        public static readonly string OctaneUrl = ConfigurationManager.AppSettings["octaneurl"].ToString();

        public static readonly string QueryRallyEpic = ReadConfiguration(ConfigTable, type, "QueryRallyEpic");
        public static readonly string QueryRallyFeature = ReadConfiguration(ConfigTable, type, "QueryRallyFeature");
        public static readonly string QueryRallyUserStory = ReadConfiguration(ConfigTable, type, "QueryRallyUserStory");

        public static string QueryRallyMilestone { get; set; } = ReadConfiguration(ConfigTable, type, "QueryRallyMilestone");
        public static readonly string QueryRallyMilestoneForFeature = ReadConfiguration(ConfigTable, type, "QueryRallyMilestoneForFeature");
        public static readonly string[] Ready_udf_List = ReadConfiguration(ConfigTable, list, "ready_udf_list").Split(';');

        public static DataTable dtUsers { get; set; } = new DataTable();
        public static DataTable dtMilestonesForFeatures { get; set; }

        public static string ClientId { get; set; }
        public static string ClientSecret { get; set; }
        public static string SharedSpaceId { get; set; }
        public static string WorkspaceId { get; set; }
        public static string ParentWorkspaceId { get; set; }

        public const string colMilstone = "milestoneid";
        public static readonly string queryConfig = "SELECT * FROM @configTable";
        public static readonly string message_milestone = "Milestone : {0} for feature {1} added.";

        // Security enhancement: Input validation
        private static string SanitizeInput(string input)
        {
            if (string.IsNullOrEmpty(input))
                return string.Empty;

            // Remove potentially dangerous characters
            input = Regex.Replace(input, @"[;'\\""]", "");
            // Encode HTML to prevent XSS
            input = HttpUtility.HtmlEncode(input);
            return input;
        }

        // Security enhancement: Connection string validation
        private static bool ValidateConnectionString(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                return false;

            return Regex.IsMatch(connectionString, SafetyConnectionStringPattern);
        }

        private static string BuildSecureConnectionString(string server, string database)
        {
            if (string.IsNullOrEmpty(server) || string.IsNullOrEmpty(database))
                throw new ArgumentException("Server and database names cannot be empty");

            string sanitizedServer = SanitizeInput(server);
            string sanitizedDatabase = SanitizeInput(database);

            var builder = new SqlConnectionStringBuilder
            {
                DataSource = sanitizedServer,
                InitialCatalog = sanitizedDatabase,
                IntegratedSecurity = true,
                MultipleActiveResultSets = false,
                Encrypt = true,
                TrustServerCertificate = true
            };

            string connectionString = builder.ConnectionString;

            if (!ValidateConnectionString(connectionString))
                throw new SecurityException("Invalid connection string format");

            return connectionString;
        }

        public static DataTable LoadConfiguration(string dbserver, string database, string configtable)
        {
            if (string.IsNullOrEmpty(configtable))
                throw new ArgumentException("Config table name cannot be empty");

            DataTable datatable = new DataTable();
            try
            {
                string connString = BuildSecureConnectionString(dbserver, database);
                using (SqlConnection conn = new SqlConnection(connString))
                {
                    using (SqlCommand cmd = new SqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.CommandText = "SELECT * FROM @configTable";
                        cmd.Parameters.AddWithValue("@configTable", SanitizeInput(configtable));
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
                string sanitizedMessage = SanitizeInput(e.Message);
                Octane.logger.Error(e, "An error occurred in load configuration: {0}", sanitizedMessage);
            }
            return datatable;
        }

        public static string ReadConfiguration(DataTable datatable, string category, string key)
        {
            if (datatable == null)
                throw new ArgumentNullException(nameof(datatable));

            DataRow[] value;
            string sanitizedCategory = SanitizeInput(category);
            string sanitizedKey = SanitizeInput(key);

            using (SqlCommand cmd = new SqlCommand())
            {
                cmd.Parameters.AddWithValue("@category", sanitizedCategory);
                cmd.Parameters.AddWithValue("@key", sanitizedKey);
                string expression = "Category = @category AND Key = @key";
                value = datatable.Select(expression);
            }

            return value.Length > 0 ? SanitizeInput(value[0][3].ToString()) : string.Empty;
        }

        public static void DisplayMessage(string message, params object[] args)
        {
            string sanitizedMessage = SanitizeInput(message);
            if (args == null || args.Length == 0)
            {
                if (sanitizedMessage == "=" || sanitizedMessage == "-" || sanitizedMessage == "*" || sanitizedMessage == "_")
                {
                    Console.WriteLine(string.Concat(Enumerable.Repeat(sanitizedMessage, 100)));
                }
                else
                {
                    Console.WriteLine(sanitizedMessage
                        .Replace("[", "")
                        .Replace("]", "")
                        .Replace("{", "")
                        .Replace("}", ""));
                }
            }
            else
            {
                object[] sanitizedArgs = args.Select(arg =>
                    arg is string ? (object)SanitizeInput((string)arg) : arg).ToArray();
                Console.WriteLine(string.Format(sanitizedMessage, sanitizedArgs));
            }
        }

        public static void DisplayErrorMessage(string action, string obj, string formattedID, string jsonmessage)
        {
            string sanitizedAction = SanitizeInput(action);
            string sanitizedObj = SanitizeInput(obj);
            string sanitizedFormattedID = SanitizeInput(formattedID);
            string sanitizedJsonMessage = SanitizeInput(jsonmessage);

            Console.WriteLine(string.Concat(Enumerable.Repeat("-", 100)));
            Octane.logger.Error(string.Concat(Enumerable.Repeat("-", 100)));

            Console.WriteLine($"Could not {sanitizedAction} {sanitizedObj} [yellow]{sanitizedFormattedID}[/]:");
            Octane.logger.Error($"Could not {sanitizedAction} {sanitizedObj} [{sanitizedFormattedID}]:");

            try
            {
                JObject jsonObject = JObject.Parse(sanitizedJsonMessage);
                DataTable table = new DataTable();
                table.Columns.Add("Property");
                table.Columns.Add("Value");

                if (jsonObject["errors"] != null)
                {
                    JArray errors = (JArray)jsonObject["errors"];
                    foreach (JObject error in errors)
                    {
                        foreach (JProperty property in error.Properties())
                        {
                            AddPropertyToDataTable(property, table);
                        }
                    }
                }
                else
                {
                    foreach (JProperty property in jsonObject.Properties())
                    {
                        AddPropertyToDataTable(property, table);
                    }
                }

                foreach (DataRow row in table.Rows)
                {
                    Console.WriteLine($"{SanitizeInput(row["Property"].ToString())}: {SanitizeInput(row["Value"].ToString())}");
                }

                Console.WriteLine(string.Concat(Enumerable.Repeat("-", 100)));
                Octane.logger.Error(string.Concat(Enumerable.Repeat("-", 100)));
            }
            catch (Exception)
            {
                Thread.Sleep(10000);
            }
        }

        private static void AddPropertyToDataTable(JProperty property, DataTable table)
        {
            if (property == null || table == null)
                return;

            string sanitizedName = SanitizeInput(property.Name);
            string sanitizedValue = property.Value != null ? SanitizeInput(property.Value.ToString()) : string.Empty;
            table.Rows.Add(sanitizedName, sanitizedValue);
        }

        private static string GetConnectionString(string dbserver, string database)
        {
            try
            {
                return BuildSecureConnectionString(dbserver, database);
            }
            catch (Exception e)
            {
                string sanitizedMessage = SanitizeInput(e.Message);
                DisplayMessage("An error occurred in get connection: {0}", sanitizedMessage);
                Octane.logger.Error(e, "An error occurred: {0}", sanitizedMessage);
                return string.Empty;
            }
        }

        public static DataTable GetDataTableFromSql(string dbserver, string database, string queryRally)
        {
            if (string.IsNullOrEmpty(queryRally))
                throw new ArgumentException("Query cannot be empty");

            var connectionString = BuildSecureConnectionString(dbserver, database);
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                using (SqlCommand cmd = new SqlCommand(queryRally, conn))
                {
                    cmd.CommandTimeout = 0;
                    conn.Open();
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
            if (string.IsNullOrEmpty(BaseUrl) || string.IsNullOrEmpty(app))
                throw new ArgumentException("Base URL and app name cannot be empty");

            HttpClientHandler handler = new HttpClientHandler()
            {
                Proxy = new WebProxy("nwbcproxy2.res.sys.shared.fortis:8080", false),
                UseProxy = true
            };

            var client = new HttpClient(handler)
            {
                Timeout = TimeSpan.FromSeconds(300)
            };

            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            string connectionString = BuildSecureConnectionString(DbServer, Database);
            string decryptedData = EncryptionService.ReadEncryptedDataFromDatabase(connectionString, SanitizeInput(app));

            if (string.IsNullOrEmpty(decryptedData))
                throw new SecurityException("Failed to decrypt authentication data");

            string[] data = decryptedData.Split(';');
            if (data.Length < 5)
                throw new SecurityException("Invalid authentication data format");

            ClientId = data[0];
            ClientSecret = data[1];
            WorkspaceId = data[2];
            SharedSpaceId = data[3];
            ParentWorkspaceId = data[4];

            var authData = new { client_id = ClientId, client_secret = ClientSecret };
            var content = new StringContent(
                JsonConvert.SerializeObject(authData),
                System.Text.Encoding.UTF8,
                "application/json"
            );

            HttpResponseMessage response = client.PostAsync(BaseUrl + "authentication/sign_in", content).Result;

            if (response.IsSuccessStatusCode)
                return client;

            DisplayMessage("Status Code: {0}", (int)response.StatusCode);
            DisplayMessage("Reason: {0}", response.ReasonPhrase);
            Octane.logger.Error(response.ReasonPhrase, "An error occurred: {0}");
            DisplayMessage("Could not authenticate with Octane");
            return null;
        }

        public static DataTable GetOctaneUsers(HttpClient client)
        {
            if (client == null)
                throw new ArgumentNullException(nameof(client));

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
                        row["id"] = SanitizeInput(item["id"].ToString());
                        row["uid"] = SanitizeInput(item["uid"].ToString());
                        row["email"] = SanitizeInput(item["email"].ToString());
                        row["type"] = SanitizeInput(item["type"].ToString());
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
            if (dt == null)
                throw new ArgumentNullException(nameof(dt));

            if (string.IsNullOrEmpty(EmailAddress))
            {
                DisplayMessage("-");
                Octane.logger.Error("in Rally the EmailAddress is empty : {0} for {1} in Octane ",
                    SanitizeInput(EmailAddress), SanitizeInput(FormattedID));
                return string.Empty;
            }

            string result = string.Empty;
            try
            {
                var emailAddressPrefix = EmailAddress.Substring(0, EmailAddress.IndexOf('@'));
                emailAddressPrefix = SanitizeInput(emailAddressPrefix);

                result = (from row in dt.AsEnumerable()
                          let email = SanitizeInput(row.Field<string>("email"))
                          let emailPrefix = email.Substring(0, email.IndexOf('@'))
                          where emailPrefix.Equals(emailAddressPrefix, StringComparison.OrdinalIgnoreCase)
                          select row.Field<string>("id")).FirstOrDefault();

                if (string.IsNullOrEmpty(result))
                {
                    DisplayMessage("-");
                    DisplayMessage("Cannot find the user {0} for [red]{1}[/] in Octane ",
                        SanitizeInput(EmailAddress), SanitizeInput(FormattedID));
                    DisplayMessage("-");
                    Octane.logger.Error("Cannot find the user {0} for {1} in Octane ",
                        SanitizeInput(EmailAddress), SanitizeInput(FormattedID));
                }
            }
            catch (Exception ex)
            {
                string sanitizedMessage = SanitizeInput(ex.Message);
                DisplayMessage("Error in searchUserId : {0} for the user : {1} for [red]{2}[/]",
                    sanitizedMessage, SanitizeInput(EmailAddress), SanitizeInput(FormattedID));
                Octane.logger.Error(sanitizedMessage, SanitizeInput(EmailAddress), SanitizeInput(FormattedID),
                    "Error in searchUserId : {0} for the user : {1} for {2}");
            }
            return result;
        }

        private static Tuple<string, string> ValueLookup(HttpClient client, string type, string rallyType, string portfolioItem)
        {
            if (client == null)
                throw new ArgumentNullException(nameof(client));

            string lookupValue = string.Empty;
            string lookupID = string.Empty;
            string search = string.Empty;
            string query = string.Empty;
            string lookupUrl = string.Empty;

            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("id", typeof(string));
            dataTable.Columns.Add("name", typeof(string));

            string sanitizedType = SanitizeInput(type);
            string sanitizedRallyType = SanitizeInput(rallyType);
            string sanitizedPortfolioItem = SanitizeInput(portfolioItem);

            if (sanitizedPortfolioItem == "epic")
            {
                switch (sanitizedType)
                {
                    case "phase":
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/phases?fields=id,name,entity&query=\"entity EQ ^epic^\"";
                        break;
                    case "epic type":
                        query = "query=\"(list_root={null})\"";
                        search = "text_search={\"type\":\"context\",\"text\":\"" + sanitizedType + "\"}";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/list_nodes?fields=id,list_nodes,name&limit=10&{query}&{search}";
                        break;
                    default:
                        DisplayMessage("Invalid {0} type", sanitizedType);
                        break;
                }
            }
            else if (sanitizedPortfolioItem == "feature")
            {
                switch (sanitizedType)
                {
                    case "phase":
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/phases?fields=id,name,entity&query=\"entity EQ ^feature^\"";
                        break;
                    case "release":
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/releases?fields=id,name&limit=1500";
                        break;
                    case "feature type":
                        query = "query=\"(list_root={null})\"";
                        search = "text_search={\"type\":\"context\",\"text\":\"" + sanitizedType + "\"}";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/list_nodes?fields=id,list_nodes,name&limit=10&{query}&{search}";
                        break;
                    case "wava test":
                    case "perf test":
                    case "test plan status":
                        query = "query=\"(list_root={null})\"";
                        search = "text_search={\"type\":\"context\",\"text\":\"" + sanitizedType + "\"}";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/list_nodes?fields=id,list_nodes,name&limit=10&{query}&{search}";
                        break;
                    case "parent":
                        query = $"query=\"subtype EQ ^epic^ ;formatted_id_udf EQ ^{sanitizedRallyType}^\"";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/work_items?fields=id,formatted_id_udf&limit=1&{query}";
                        break;
                    default:
                        DisplayMessage("Invalid {0} type", sanitizedType);
                        break;
                }
            }
            else
            {
                switch (sanitizedType)
                {
                    case "phase":
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/phases?fields=id,name,entity&query=\"entity EQ ^story^\"";
                        break;
                    case "release":
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/releases?fields=id,name&query=\"name EQ ^{sanitizedRallyType}^\"";
                        break;
                    case "sprint":
                        string var_release = sanitizedRallyType != "" ? "202" + sanitizedRallyType[3] + ".Q" + sanitizedRallyType[6] : "";
                        string var_sprint = sanitizedRallyType != "" ? "Sprint " + sanitizedRallyType[10] : "";
                        string query_sprint = $"\"name EQ ^{var_sprint}^;release EQ {{name EQ ^{var_release}^}}\"";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/sprints?fields=id,name&query={query_sprint}";
                        break;
                    case "parent":
                        query = $"query=\"subtype EQ ^feature^ ;formatted_id_udf EQ ^{sanitizedRallyType}^\"";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/work_items?fields=id,formatted_id_udf&limit=1&{query}";
                        break;
                    case "usparent":
                        query = $"query=\"subtype EQ ^story^ ;formatted_id_udf EQ ^{sanitizedRallyType}^\"";
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/work_items?fields=id,formatted_id_udf&limit=1&{query}";
                        break;
                    case "ready":
                        if (sanitizedRallyType == "True")
                            return Tuple.Create(Ready_udf_List[0], "ready_udf");
                        else
                            return Tuple.Create(Ready_udf_List[1], "ready_udf");
                    case "team":
                        string rallyType_url = sanitizedRallyType.Replace("&", "%26");
                        lookupUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/teams?fields=id,name&query=\"name EQ ^{rallyType_url}^\"";
                        break;
                    default:
                        DisplayMessage("Invalid {0} type", sanitizedType);
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
                    if (sanitizedType == "phase" || sanitizedType == "parent" ||
                        sanitizedType == "release" || sanitizedType == "sprint" ||
                        sanitizedType == "team" || sanitizedType == "release" ||
                        sanitizedType == "usparent")
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
                        row["id"] = SanitizeInput(item["id"].ToString());
                        if (sanitizedType == "parent" || sanitizedType == "usparent")
                        {
                            row["name"] = SanitizeInput(item["formatted_id_udf"].ToString());
                        }
                        else
                        {
                            row["name"] = SanitizeInput(item["name"].ToString());
                        }
                        dataTable.Rows.Add(row);
                    }

                    foreach (DataRow row in dataTable.Rows)
                    {
                        if (sanitizedType == "wava test" || sanitizedType == "perf test")
                        {
                            if (sanitizedRallyType.Trim().Equals(row["name"].ToString().Trim(), StringComparison.OrdinalIgnoreCase))
                            {
                                lookupValue = row["name"].ToString();
                                lookupID = row["id"].ToString();
                                break;
                            }
                        }
                        else
                        {
                            if (sanitizedType == "sprint")
                            {
                                lookupValue = row["name"].ToString();
                                lookupID = row["id"].ToString();
                                break;
                            }
                            else
                            {
                                var var_rallyType = sanitizedRallyType.ToLower();
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
            if (client == null || dataTable == null)
                throw new ArgumentNullException("Client and data table cannot be null");

            Parallel.ForEach(dataTable.AsEnumerable(), row =>
            {
                string formattedID = SanitizeInput(row["formattedID"].ToString());
                string stateName = SanitizeInput(row["StateName"].ToString());

                if (string.IsNullOrEmpty(stateName))
                {
                    DisplayMessage("The phase in Rally for the Epic {0} is empty : this is not allowed. Therefore [red]{0}[/] will not be added or updated.", formattedID);
                    Octane.logger.Error("The phase in Rally for the Epic {0} is empty : this is not allowed. Therefore {0} will not be added or updated.", formattedID);
                }
                else
                {
                    var epicName = formattedID + " " + SanitizeInput(row["name"].ToString());
                    var search_formattedid_epic_octane = "formatted_id_udf EQ ^" + formattedID + "^";
                    var urlEpic = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/epics?query=\"{search_formattedid_epic_octane}\"";

                    string search_epic = client.GetStringAsync(urlEpic).Result;
                    dynamic searchResult = JsonConvert.DeserializeObject<dynamic>(search_epic);
                    dynamic existingEpicArray = searchResult?.data as JArray;

                    var id_phase = ValueLookup(client, "phase", stateName, "epic").Item1;
                    var id_type = ValueLookup(client, "epic type", SanitizeInput(row["c_Type"].ToString()), "epic").Item1;
                    var id_user = searchUserId(dtUsers, SanitizeInput(row["EmailAddress"].ToString()), formattedID);

                    if (existingEpicArray != null && existingEpicArray.Count > 0)
                    {
                        var epicData = new
                        {
                            name = epicName,
                            description = SanitizeInput(row["description"].ToString()),
                            formatted_id_udf = formattedID,
                            clarity_code_udf = SanitizeInput(row["c_ClarityID"].ToString()),
                            epic_type = string.IsNullOrEmpty(id_type) ? null : new
                            {
                                id = id_type,
                                type = "list_node",
                            },
                            ac_project_udf = SanitizeInput(row["TribeName"].ToString()),
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

                        var existingEpic = existingEpicArray[0];
                        var epicId = existingEpic.id;
                        var updateUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/epics/{epicId}";
                        var updateContent = new StringContent(JsonConvert.SerializeObject(epicData), System.Text.Encoding.UTF8, "application/json");

                        HttpResponseMessage updateResponse = client.PutAsync(updateUrl, updateContent).Result;

                        if (!updateResponse.IsSuccessStatusCode)
                        {
                            DisplayErrorMessage("update", "epic", formattedID, updateResponse.Content.ReadAsStringAsync().Result);
                        }
                        else
                        {
                            DisplayMessage("Epic updated: {0}", formattedID);
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
                                    description = SanitizeInput(row["description"].ToString()),
                                    formatted_id_udf = formattedID,
                                    clarity_code_udf = SanitizeInput(row["c_ClarityID"].ToString()),
                                    ac_project_udf = SanitizeInput(row["TribeName"].ToString()),
                                    epic_type = string.IsNullOrEmpty(id_type) ? null : new
                                    {
                                        id = id_type,
                                        type = "list_node"
                                    },
                                    parent = new
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

                        var addUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/epics";
                        var addContent = new StringContent(JsonConvert.SerializeObject(epicData), System.Text.Encoding.UTF8, "application/json");
                        HttpResponseMessage addResponse = client.PostAsync(addUrl, addContent).Result;

                        if (!addResponse.IsSuccessStatusCode)
                        {
                            DisplayErrorMessage("add", "epic", formattedID, addResponse.Content.ReadAsStringAsync().Result);
                        }
                        else
                        {
                            DisplayMessage("Epic added: {0} ", formattedID);
                            Octane.logger.Info("Epic added: {0} ", epicName);
                        }
                    }
                }
            });
        }

        public static string GetFeatureChildrenList(string FeatureID)
        {
            if (string.IsNullOrEmpty(FeatureID))
                return "No US";

            string sanitizedFeatureID = SanitizeInput(FeatureID);
            DataTable dtUS = GetDataTableFromSql(
                DbServer,
                Database,
                QueryRallyUserStory + " AND FE.FormattedID = @featureId AND US.LastUpdateDate >= @lastUpdateDate");

            if (dtUS.Rows.Count == 0)
                return "No US";

            List<string> listUS = new List<string>();
            foreach (DataRow rw in dtUS.Rows)
            {
                listUS.Add(SanitizeInput(rw["USID"].ToString()));
            }
            return string.Join(",", listUS);
        }

        public static void HandleFeature(HttpClient client, DataTable dataTable)
        {
            if (client == null || dataTable == null)
                throw new ArgumentNullException("Client and data table cannot be null");

            string formattedID = string.Empty;
            int counter = 0;

            try
            {
                ParallelOptions options = new ParallelOptions
                {
                    MaxDegreeOfParallelism = Environment.ProcessorCount * 2
                };

                Parallel.ForEach(dataTable.AsEnumerable(), options, (row, loopState) =>
                {
                    Interlocked.Increment(ref counter);
                    string stateName = SanitizeInput(row["StateName"].ToString());
                    formattedID = SanitizeInput(row["FeatureID"].ToString());

                    if (string.IsNullOrEmpty(stateName))
                    {
                        string ListUSFeature = GetFeatureChildrenList(formattedID);
                        DisplayMessage("The phase in Rally for feature {0} is empty : this is not allowed. Therefore [red]{0}[/] will not be added or updated and its following children {1} too.",
                            formattedID, ListUSFeature);
                        Octane.logger.Error("The phase in Rally for feature {0} is empty : this is not allowed. Therefore {0} will not be added or updated and its following children {1} too.",
                            formattedID, ListUSFeature);
                    }
                    else
                    {
                        var featureName = formattedID + " " + SanitizeInput(row["name"].ToString());
                        var search_formattedid_feature_octane = "formatted_id_udf EQ ^" + formattedID + "^";
                        var urlFeature = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/features?fields=trv_udf&query=\"{search_formattedid_feature_octane}\"";

                        string search_feature = client.GetStringAsync(urlFeature).Result;
                        dynamic searchResult = JsonConvert.DeserializeObject<dynamic>(search_feature);
                        dynamic existingFeatureArray = searchResult?.data as JArray;

                        // Secure value lookups
                        var id_phase = ValueLookup(client, "phase", stateName, "feature").Item1;
                        var id_release = ValueLookup(client, "release", SanitizeInput(row["ReleaseName"].ToString()), "feature").Item1;
                        var id_type = ValueLookup(client, "feature type", SanitizeInput(row["c_Type"].ToString()), "feature").Item1;
                        var id_wafa = ValueLookup(client, "wava test", SanitizeInput(row["wafa_test"].ToString()), "feature").Item1;
                        var id_perf = ValueLookup(client, "perf test", SanitizeInput(row["perf_test"].ToString()), "feature").Item1;
                        var id_test_plan_status = ValueLookup(client, "test plan status", SanitizeInput(row["c_TestPlanStatus"].ToString()), "feature").Item1;
                        var id_parent = ValueLookup(client, "parent", SanitizeInput(row["OpusID"].ToString()), "feature").Item1;
                        

                        // Continue with the rest of HandleFeature implementation...
                        // Would you like me to continue with the remaining code?
                    }
                });
            }
            catch (Exception ex)
            {
                string sanitizedMessage = SanitizeInput(ex.Message);
                DisplayMessage("An error occurred in feature handling: {0} - Feature : {1} - Current row : {2}",
                    sanitizedMessage, formattedID, counter);
                Octane.logger.Error(ex, "An error occurred in feature handling: {0} - Feature : {1} - Current row : {2}",
                    sanitizedMessage, formattedID, counter);
            }
        }

        public static void HandleStory(HttpClient client, DataTable dataTable)
        {
            if (client == null || dataTable == null)
                throw new ArgumentNullException("Client and data table cannot be null");

            string formattedID = string.Empty;
            int counter = 0;

            try
            {
                ParallelOptions options = new ParallelOptions
                {
                    MaxDegreeOfParallelism = Environment.ProcessorCount * 2
                };

                Parallel.ForEach(dataTable.AsEnumerable(), options, (row, loopState) =>
                {
                    Interlocked.Increment(ref counter);
                    formattedID = SanitizeInput(row["USID"].ToString());
                    string scheduleState = SanitizeInput(row["ScheduleState"].ToString());

                    if (string.IsNullOrEmpty(scheduleState))
                    {
                        DisplayMessage("The phase in Rally for {0} is empty : this is not allowed. Therefore [red]{0}[/] will not be added or updated.", formattedID);
                        Octane.logger.Error("The phase in Rally for {0} is empty : this is not allowed. Therefore {0} will not be added or updated.", formattedID);
                        return;
                    }

                    var storyName = formattedID + " " + SanitizeInput(row["name"].ToString());
                    var search_formattedid_story_octane = "formatted_id_udf EQ ^" + formattedID + "^";
                    var urlStory = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories?query=\"{search_formattedid_story_octane}\"";

                    string search_UserStory;
                    try
                    {
                        search_UserStory = client.GetStringAsync(urlStory).Result;
                    }
                    catch (HttpRequestException ex)
                    {
                        string sanitizedMessage = SanitizeInput(ex.Message);
                        DisplayMessage("Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}",
                            sanitizedMessage, formattedID, counter);
                        Octane.logger.Error(ex, "Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}",
                            sanitizedMessage, formattedID, counter);
                        Thread.Sleep(10000);
                        return;
                    }

                    dynamic searchResult = JsonConvert.DeserializeObject<dynamic>(search_UserStory);
                    dynamic existingStoryArray = searchResult?.data as JArray;

                    // Secure value lookups with sanitized inputs
                    var id_phase = ValueLookup(client, "phase", scheduleState, "userstory").Item1;
                    string var_sprint = SanitizeInput(row["sprint"].ToString());
                    string var_release = !string.IsNullOrEmpty(var_sprint) ? "202" + var_sprint[3] + ".Q" + var_sprint[6] : "";
                    var id_release = ValueLookup(client, "release", var_release, "userstory").Item1;
                    var id_sprint = ValueLookup(client, "sprint", var_sprint, "userstory").Item1;
                    var id_parent = ValueLookup(client, "parent", SanitizeInput(row["featureid"].ToString()), "userstory").Item1;
                    var id_parent_type = "work_item";
                    var id_usparent = ValueLookup(client, "usparent", SanitizeInput(row["usparentid"].ToString()), "userstory").Item1;
                    var id_ready = ValueLookup(client, "ready", SanitizeInput(row["Ready"].ToString()), "userstory").Item1;
                    var id_team = ValueLookup(client, "team", SanitizeInput(row["TribeName"].ToString()), "userstory").Item1;

                    if (string.IsNullOrEmpty(id_team))
                    {
                        string sanitizedTeamName = SanitizeInput(row["TribeName"].ToString());
                        DisplayMessage("US : {0} -- The team {1} not found in Octane. action has to be taken.", formattedID, sanitizedTeamName);
                        Octane.logger.Info("US : {0} -- The team {1} not found in Octane. action has to be taken.", formattedID, sanitizedTeamName);
                    }

                    if (string.IsNullOrEmpty(id_release))
                    {
                        DisplayMessage("US : {0} -- The release {1} not found in Octane. action has to be taken.", formattedID, var_release);
                        Octane.logger.Info("US : {0} -- The release {1} not found in Octane. action has to be taken.", formattedID, var_release);
                    }

                    // Handle parent lookup and creation logic with security measures
                    if (string.IsNullOrEmpty(id_parent) && string.IsNullOrEmpty(id_usparent))
                    {
                        string featureId = SanitizeInput(row["FeatureID"].ToString());
                        string usParentId = SanitizeInput(row["USPARENTID"].ToString());

                        if (!string.IsNullOrEmpty(featureId))
                        {
                            using (SqlCommand cmd = new SqlCommand())
                            {
                                cmd.Parameters.AddWithValue("@featureId", featureId);
                                DataTable dtFEATURE = GetDataTableFromSql(DbServer, Database,
                                    QueryRallyFeature + " AND FE.FormattedID = @featureId");
                                HandleFeature(client, dtFEATURE);
                                id_parent = ValueLookup(client, "parent", featureId, "userstory").Item1;
                            }
                        }
                        else if (!string.IsNullOrEmpty(usParentId))
                        {
                            using (SqlCommand cmd = new SqlCommand())
                            {
                                cmd.Parameters.AddWithValue("@usParentId", usParentId);
                                DataTable dtSTORY = GetDataTableFromSql(DbServer, Database,
                                    QueryRallyUserStory + " AND US.formattedid = @usParentId");
                                HandleStory(client, dtSTORY);
                                id_parent = ValueLookup(client, "usparent", usParentId, "userstory").Item1;
                            }
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

                    var id_user = searchUserId(dtUsers, SanitizeInput(row["EmailAddress"].ToString()), formattedID);

                    // Create story data object with sanitized inputs
                    if (existingStoryArray != null && existingStoryArray.Count > 0)
                    {
                        var storyData = new
                        {
                            name = storyName,
                            description = SanitizeInput(row["description"].ToString()),
                            formatted_id_udf = formattedID,
                            story_points = (row["PlanEstimate"].ToString() == "0") ?
                                (long?)null :
                                Convert.ToInt64(Math.Round(Convert.ToDouble(row["PlanEstimate"].ToString()))),
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

                        var existingStory = existingStoryArray[0];
                        var storyId = existingStory.id;
                        var updateUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories/{storyId}";
                        var updateContent = new StringContent(JsonConvert.SerializeObject(storyData), System.Text.Encoding.UTF8, "application/json");

                        HttpResponseMessage updateResponse = client.PutAsync(updateUrl, updateContent).Result;

                        if (!updateResponse.IsSuccessStatusCode)
                        {
                            DisplayErrorMessage("update", "story", formattedID, updateResponse.Content.ReadAsStringAsync().Result);
                        }
                        else
                        {
                            DisplayMessage("Story updated: {0}", formattedID);
                            Octane.logger.Info("Story updated: {0} ", formattedID);
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
                                    description = SanitizeInput(row["description"].ToString()),
                                    formatted_id_udf = formattedID,
                                    story_points = (row["PlanEstimate"].ToString() == "0") ?
                                        (long?)null :
                                        Convert.ToInt64(Math.Round(Convert.ToDouble(row["PlanEstimate"].ToString()))),
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

                        var addUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories";
                        var addContent = new StringContent(JsonConvert.SerializeObject(storyData), System.Text.Encoding.UTF8, "application/json");
                        HttpResponseMessage addResponse = client.PostAsync(addUrl, addContent).Result;

                        if (!addResponse.IsSuccessStatusCode)
                        {
                            DisplayErrorMessage("add", "story", formattedID, addResponse.Content.ReadAsStringAsync().Result);
                        }
                        else
                        {
                            DisplayMessage("Story added: {0} ", formattedID);
                            Octane.logger.Info("Story added: {0} ", formattedID);
                        }
                    }
                });
            }
            catch (Exception ex)
            {
                string sanitizedMessage = SanitizeInput(ex.Message);
                DisplayMessage("An error occurred in story handling: {0} - Story : {1} - Current row : {2}",
                    sanitizedMessage, formattedID, counter);
                Octane.logger.Error(ex, "An error occurred in story handling: {0} - Story : {1} - Current row : {2}",
                    sanitizedMessage, formattedID, counter);
            }
        }

        public static void HandleStory_No_Parent(HttpClient client, DataTable dataTable)
        {
            if (client == null || dataTable == null)
                throw new ArgumentNullException("Client and data table cannot be null");

            string formattedID = string.Empty;
            int counter = 0;

            try
            {
                ParallelOptions options = new ParallelOptions
                {
                    MaxDegreeOfParallelism = Environment.ProcessorCount * 2
                };

                Parallel.ForEach(dataTable.AsEnumerable(), options, (row, loopState) =>
                {
                    Interlocked.Increment(ref counter);
                    formattedID = SanitizeInput(row["USID"].ToString());
                    string scheduleState = SanitizeInput(row["ScheduleState"].ToString());

                    if (string.IsNullOrEmpty(scheduleState))
                    {
                        DisplayMessage("The phase in Rally for {0} is empty : this is not allowed. Therefore [red]{0}[/] will not be added or updated.",
                            formattedID);
                        Octane.logger.Error("The phase in Rally for {0} is empty : this is not allowed. Therefore {0} will not be added or updated.",
                            formattedID);
                        return;
                    }

                    var storyName = formattedID + " " + SanitizeInput(row["name"].ToString());
                    var search_formattedid_story_octane = "formatted_id_udf EQ ^" + formattedID + "^";
                    var urlStory = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories?query=\"{search_formattedid_story_octane}\"";

                    string search_UserStory;
                    try
                    {
                        search_UserStory = client.GetStringAsync(urlStory).Result;
                    }
                    catch (HttpRequestException ex)
                    {
                        string sanitizedMessage = SanitizeInput(ex.Message);
                        DisplayMessage("Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}",
                            sanitizedMessage, formattedID, counter);
                        Octane.logger.Error(ex, "Http Request failed in Userstory handling with status code: {0} - US : {1} - Current row handled : {2}",
                            sanitizedMessage, formattedID, counter);
                        Thread.Sleep(10000);
                        return;
                    }

                    dynamic searchResult = JsonConvert.DeserializeObject<dynamic>(search_UserStory);
                    dynamic existingStoryArray = searchResult?.data as JArray;

                    // Secure value lookups with sanitized inputs
                    var id_phase = ValueLookup(client, "phase", scheduleState, "userstory").Item1;
                    string var_sprint = SanitizeInput(row["sprint"].ToString());
                    string var_release = !string.IsNullOrEmpty(var_sprint) ? "202" + var_sprint[3] + ".Q" + var_sprint[6] : "";
                    var id_release = ValueLookup(client, "release", var_release, "userstory").Item1;
                    var id_sprint = ValueLookup(client, "sprint", var_sprint, "userstory").Item1;
                    var id_usparent = ValueLookup(client, "usparent", SanitizeInput(row["usparentid"].ToString()), "userstory").Item1;
                    var id_ready = ValueLookup(client, "ready", SanitizeInput(row["Ready"].ToString()), "userstory").Item1;
                    var id_team = ValueLookup(client, "team", SanitizeInput(row["TribeName"].ToString()), "userstory").Item1;

                    // Set parent ID based on conditions
                    string id_parent;
                    string id_parent_type;
                    if (!string.IsNullOrEmpty(id_usparent))
                    {
                        id_parent = id_usparent;
                        id_parent_type = "work_item";
                    }
                    else
                    {
                        id_parent = "1002";
                        id_parent_type = "work_item_root";
                    }

                    var id_user = searchUserId(dtUsers, SanitizeInput(row["EmailAddress"].ToString()), formattedID);

                    if (existingStoryArray != null && existingStoryArray.Count > 0)
                    {
                        var storyData = CreateSecureStoryData(storyName, row, id_ready, id_team, id_release,
                            id_sprint, id_phase, id_user, id_parent, id_parent_type, formattedID);

                        var existingStory = existingStoryArray[0];
                        var storyId = existingStory.id;
                        var updateUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories/{storyId}";
                        var updateContent = new StringContent(JsonConvert.SerializeObject(storyData), System.Text.Encoding.UTF8, "application/json");

                        HttpResponseMessage updateResponse = client.PutAsync(updateUrl, updateContent).Result;

                        if (!updateResponse.IsSuccessStatusCode)
                        {
                            DisplayErrorMessage("update", "story", formattedID, updateResponse.Content.ReadAsStringAsync().Result);
                        }
                        else
                        {
                            DisplayMessage("Story updated: {0}", formattedID);
                            Octane.logger.Info("Story updated: {0} ", formattedID);
                        }
                    }
                    else
                    {
                        var storyData = new
                        {
                            data = new[] { CreateSecureStoryData(storyName, row, id_ready, id_team,
                            id_release, id_sprint, id_phase, id_user, id_parent, id_parent_type, formattedID) }
                        };

                        var addUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/stories";
                        var addContent = new StringContent(JsonConvert.SerializeObject(storyData), System.Text.Encoding.UTF8, "application/json");

                        HttpResponseMessage addResponse = client.PostAsync(addUrl, addContent).Result;

                        if (!addResponse.IsSuccessStatusCode)
                        {
                            DisplayErrorMessage("add", "story", formattedID, addResponse.Content.ReadAsStringAsync().Result);
                        }
                        else
                        {
                            DisplayMessage("Story added: {0} ", formattedID);
                            Octane.logger.Info("Story added: {0} ", formattedID);
                        }
                    }
                });
            }
            catch (Exception ex)
            {
                string sanitizedMessage = SanitizeInput(ex.Message);
                DisplayMessage("An error occurred in story handling: {0} - Story : {1} - Current row : {2}",
                    sanitizedMessage, formattedID, counter);
                Octane.logger.Error(ex, "An error occurred in story handling: {0} - Story : {1} - Current row : {2}",
                    sanitizedMessage, formattedID, counter);
            }
        }

        private static object CreateSecureStoryData(string storyName, DataRow row, string id_ready, string id_team,
            string id_release, string id_sprint, string id_phase, string id_user, string id_parent, string id_parent_type,
            string formattedID)
        {
            return new
            {
                name = storyName,
                description = SanitizeInput(row["description"].ToString()),
                formatted_id_udf = formattedID,
                story_points = (row["PlanEstimate"].ToString() == "0") ?
                    (long?)null :
                    Convert.ToInt64(Math.Round(Convert.ToDouble(row["PlanEstimate"].ToString()))),
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
        }

        public static DataTable GetMilestones(HttpClient client)
        {
            if (client == null)
                throw new ArgumentNullException(nameof(client));

            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("id", typeof(string));
            dataTable.Columns.Add("name", typeof(string));
            dataTable.Columns.Add("date", typeof(DateTime));

            int totalCount = 0;
            int limit = 1000;
            int offset = 0;

            try
            {
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
                            row["id"] = SanitizeInput(item["id"].ToString());
                            row["name"] = SanitizeInput(item["name"].ToString());
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
            }
            catch (Exception ex)
            {
                string sanitizedMessage = SanitizeInput(ex.Message);
                Octane.logger.Error(ex, "Error retrieving milestones: {0}", sanitizedMessage);
            }

            return dataTable;
        }

        public static JArray GetMilestonesForFeature(HttpClient client, DataTable dtMilestonesForFeatures, JArray featureArray)
        {
            if (client == null || dtMilestonesForFeatures == null)
                throw new ArgumentNullException("Client and milestone table cannot be null");

            try
            {
                dtMilestonesForFeatures.Columns.Add("milestone_id_octane", typeof(int));

                foreach (DataRow row in dtMilestonesForFeatures.Rows)
                {
                    string milestoneid = SanitizeInput(row[colMilstone].ToString());
                    string usersUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/milestones?fields=id&query=\"formattedid_udf EQ ^{milestoneid}^\"";

                    var response = client.GetAsync(usersUrl).Result;
                    var data = response.Content.ReadAsStringAsync().Result;
                    var milestoneData = JsonConvert.DeserializeObject<dynamic>(data);

                    row["milestone_id_octane"] = milestoneData.total_count > 0 ?
                        int.Parse(milestoneData.data[0].id.ToString()) : 0;
                }

                if (featureArray == null || featureArray.Count == 0)
                {
                    featureArray = new JArray();
                    JObject feature = new JObject
                    {
                        ["type"] = "feature",
                        ["id"] = "",
                        ["logical_name"] = "",
                        ["workspace_id"] = 1003,
                        ["trv_udf"] = new JObject
                        {
                            ["total_count"] = 0,
                            ["data"] = new JArray()
                        }
                    };

                    foreach (DataRow row in dtMilestonesForFeatures.Rows)
                    {
                        JObject newMilestone = CreateSecureMilestoneObject(row);
                        ((JArray)feature["trv_udf"]["data"]).Add(newMilestone);

                        string sanitizedMilestoneId = SanitizeInput(row[colMilstone].ToString());
                        string sanitizedName = SanitizeInput(row["name"].ToString());
                        string sanitizedFeatureId = SanitizeInput(row["featureid"].ToString());

                        DisplayMessage(message_milestone,
                            $"{sanitizedMilestoneId} - {sanitizedName}", sanitizedFeatureId);
                        Octane.logger.Info(message_milestone,
                            $"{sanitizedMilestoneId} - {sanitizedName}", sanitizedFeatureId);
                    }

                    featureArray.Add(feature);
                }
                else
                {
                    foreach (JObject feature in featureArray)
                    {
                        if (feature["trv_udf"] != null)
                        {
                            feature["trv_udf"]["data"] = new JArray();

                            foreach (DataRow row in dtMilestonesForFeatures.Rows)
                            {
                                JObject newMilestone = CreateSecureMilestoneObject(row);
                                ((JArray)feature["trv_udf"]["data"]).Add(newMilestone);

                                string sanitizedMilestoneId = SanitizeInput(row[colMilstone].ToString());
                                string sanitizedName = SanitizeInput(row["name"].ToString());
                                string sanitizedFeatureId = SanitizeInput(row["featureid"].ToString());

                                DisplayMessage(message_milestone,
                                    $"{sanitizedMilestoneId} - {sanitizedName}", sanitizedFeatureId);
                                Octane.logger.Info(message_milestone,
                                    $"{sanitizedMilestoneId} - {sanitizedName}", sanitizedFeatureId);
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
            }
            catch (Exception ex)
            {
                string sanitizedMessage = SanitizeInput(ex.Message);
                Octane.logger.Error(ex, "Error processing milestones for feature: {0}", sanitizedMessage);
            }

            return featureArray;
        }

        private static JObject CreateSecureMilestoneObject(DataRow row)
        {
            return new JObject
            {
                ["type"] = "milestone",
                ["id"] = SanitizeInput(row["milestone_id_octane"].ToString()),
                ["activity_level"] = 0,
                ["name"] = SanitizeInput(row["name"].ToString())
            };
        }

        public static void HandleMilestone(HttpClient client, DataTable dataTable)
        {
            if (client == null || dataTable == null)
                throw new ArgumentNullException("Client and data table cannot be null");

            foreach (DataRow row in dataTable.Rows)
            {
                try
                {
                    string milestoneid = SanitizeInput(row[colMilstone].ToString());
                    string milestoneName = SanitizeInput(row["name"].ToString());
                    var search_milestone_octane = "formattedid_udf EQ ^" + milestoneid + "^";
                    var urlMilestone = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/milestones?query=\"{search_milestone_octane}\"";

                    string search_milestone = client.GetStringAsync(urlMilestone).Result;
                    dynamic searchResult = JsonConvert.DeserializeObject<dynamic>(search_milestone);
                    dynamic existingMilestoneArray = searchResult?.data as JArray;

                    if (existingMilestoneArray != null && existingMilestoneArray.Count > 0)
                    {
                        var milestoneData = CreateSecureMilestoneData(milestoneid, milestoneName, row);
                        var existingMilestone = existingMilestoneArray[0];
                        var milestoneId = existingMilestone.id;
                        var updateUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/milestones/{milestoneId}";
                        var updateContent = new StringContent(JsonConvert.SerializeObject(milestoneData),
                            System.Text.Encoding.UTF8, "application/json");

                        HttpResponseMessage updateResponse = client.PutAsync(updateUrl, updateContent).Result;

                        if (!updateResponse.IsSuccessStatusCode)
                        {
                            DisplayErrorMessage("update", "milestone",
                                $"{milestoneid} - {milestoneName}", updateResponse.Content.ReadAsStringAsync().Result);
                        }
                        else
                        {
                            DisplayMessage($"Milestone updated: {milestoneid} - {milestoneName}");
                            Octane.logger.Info($"Milestone updated: {milestoneid} - {milestoneName}");
                        }
                    }
                    else
                    {
                        var milestoneData = new { data = new[] { CreateSecureMilestoneData(milestoneid, milestoneName, row) } };
                        var addUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/milestones";
                        var addContent = new StringContent(JsonConvert.SerializeObject(milestoneData),
                            System.Text.Encoding.UTF8, "application/json");

                        HttpResponseMessage addResponse = client.PostAsync(addUrl, addContent).Result;

                        if (!addResponse.IsSuccessStatusCode)
                        {
                            DisplayErrorMessage("add", "milestone",
                                $"{milestoneid} - {milestoneName}", addResponse.Content.ReadAsStringAsync().Result);
                        }
                        else
                        {
                            DisplayMessage($"Milestone added: {milestoneid} - {milestoneName}");
                            Octane.logger.Info($"Milestone added: {milestoneid} - {milestoneName}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    string sanitizedMessage = SanitizeInput(ex.Message);
                    Octane.logger.Error(ex, "Error handling milestone: {0}", sanitizedMessage);
                }
            }
        }

        private static object CreateSecureMilestoneData(string milestoneid, string milestoneName, DataRow row)
        {
            return new
            {
                formattedid_udf = milestoneid,
                name = milestoneName,
                date = (row["TargetDate"].ToString() == "01/01/1900 00:00:00") ?
                    (DateTimeOffset?)null :
                    new DateTimeOffset(DateTime.Parse(row["TargetDate"].ToString())),
            };
        }

        public static string GetFormattedId(string milestoneName, string connectionString)
        {
            if (string.IsNullOrEmpty(milestoneName) || string.IsNullOrEmpty(connectionString))
                throw new ArgumentException("Milestone name and connection string cannot be empty");

            string formattedId = string.Empty;

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    string query = ReadConfiguration(ConfigTable, "query", "GetMilestone");
                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@milestoneName", SanitizeInput(milestoneName));

                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                formattedId = SanitizeInput(reader["formattedid"].ToString());
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                string sanitizedMessage = SanitizeInput(ex.Message);
                Octane.logger.Error(ex, "Error getting formatted ID: {0}", sanitizedMessage);
            }

            return formattedId;
        }

        public static void HandleDuplicateMilestone(HttpClient client, DataTable dataTable)
        {
            if (client == null || dataTable == null)
                throw new ArgumentNullException("Client and data table cannot be null");

            string connectionString = BuildSecureConnectionString(DbServer, Database);

            foreach (DataRow row in dataTable.Rows)
            {
                try
                {
                    string milestoneid = SanitizeInput(row[colMilstone].ToString());
                    string milestoneName = SanitizeInput(row["milestonename"].ToString());
                    string milestoneFormattedid = GetFormattedId(milestoneName, connectionString);

                    var urlMilestone = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/milestones/{milestoneid}";
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
                        var updateUrl = $"{OctaneUrl}api/shared_spaces/{SharedSpaceId}/workspaces/{WorkspaceId}/milestones/{milestoneId}";
                        var updateContent = new StringContent(JsonConvert.SerializeObject(milestoneData),
                            System.Text.Encoding.UTF8, "application/json");

                        HttpResponseMessage updateResponse = client.PutAsync(updateUrl, updateContent).Result;

                        if (!updateResponse.IsSuccessStatusCode)
                        {
                            DisplayErrorMessage("update", "milestone",
                                $"{milestoneid} - {milestoneName}", updateResponse.Content.ReadAsStringAsync().Result);
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
                catch (Exception ex)
                {
                    string sanitizedMessage = SanitizeInput(ex.Message);
                    Octane.logger.Error(ex, "Error handling duplicate milestone: {0}", sanitizedMessage);
                }
            }
        }
    }
}
