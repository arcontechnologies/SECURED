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
