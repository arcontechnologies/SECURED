public static string GetFormattedId(string milestoneName, string dbServer, string database)
{
   if (string.IsNullOrEmpty(milestoneName))
       throw new ArgumentException("Milestone name cannot be empty");

   string formattedId = string.Empty;
   try
   {
       string connectionString = Helpers.GetConnectionString(dbServer, database);
       using (SqlConnection connection = new SqlConnection(connectionString))
       {
           connection.Open();
           using (SqlCommand command = new SqlCommand())
           {
               command.Connection = connection;
               command.CommandType = CommandType.Text;
               command.CommandTimeout = DEFAULT_COMMAND_TIMEOUT;
               command.CommandText = "SELECT formattedid FROM Milestones WHERE name = @milestoneName";
               command.Parameters.Add("@milestoneName", SqlDbType.NVarChar, 256).Value = SanitizeInput(milestoneName);
               
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
