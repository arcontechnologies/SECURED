The method LogMessage() in octane.cs writes unvalidated user input to the log on line 343. An attacker could take advantage of this behavior to forge log entries or inject malicious content into the log.

public static void LogMessage(Logger logger, string message, LogLevel level)
        {
            if (logger == null || string.IsNullOrWhiteSpace(message))
                return;

            string sanitizedMessage = SanitizeInput(message);
            logger.Info(sanitizedMessage);

        }

===========================================================
Access Control: Database

Without proper access control, the method LoadConfiguration() in Helpers.cs can execute a SQL statement on line 117 that contains an attacker-controlled primary key, thereby allowing the attacker to access unauthorized records.


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

=========================================================================================
Access Control: Database

Without proper access control, the method GetDataTableFromSql() in Helpers.cs can execute a SQL statement on line 266 that contains an attacker-controlled primary key, thereby allowing the attacker to access unauthorized records.

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
