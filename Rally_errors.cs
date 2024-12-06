Access Control: Database

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
                        CommandText = $"SELECT * FROM @TableName"
                    };
                    query.Parameters.AddWithValue("@TableName", sanitizedTableName);

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

Without proper access control, the method LoadConfiguration() in Helpers.cs can execute a SQL statement on line 238 that contains an attacker-controlled primary key, thereby allowing the attacker to access unauthorized records.

Rather than relying on the presentation layer to restrict values submitted by the user, access control should be handled by the application and database layers. Under no circumstances should a user be allowed to retrieve or modify a row in the database without the appropriate permissions. Every query that accesses the database should enforce this policy, which can often be accomplished by simply including the current authenticated username as part of the query.


===========================================================================================================================

        Log Forging

        The method WebRequestWithToken() in Helpers.cs writes unvalidated user input to the log on line 357. An attacker could take advantage of this behavior to forge log entries or inject malicious content into the log.

        Prevent log forging attacks with indirection: create a set of legitimate log entries that correspond to different events that must be logged and only log entries from this set. To capture dynamic content, such as users logging out of the system, always use server-controlled values rather than user-supplied data. This ensures that the input provided by the user is never used directly in a log entry.

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
                        string sanitizedMessage = SanitizeErrorMessage(errorMessage);
                        RallyLoad.logger.Error(sanitizedMessage);
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
