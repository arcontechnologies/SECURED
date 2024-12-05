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


