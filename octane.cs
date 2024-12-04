using System;
using System.Data;
using System.Net.Http;
using System.Linq;
using System.Threading;
using System.Configuration;
using NLog;
using System.Text.RegularExpressions;
using System.Web;

namespace OctaneSync
{
    public static class Octane
    {
        public static string QueryRallyStoryWhere = string.Empty;
        public static string QueryRallyStoryWhere_ORG = string.Empty;
        public static readonly string LastUpdateDate = DateTime.Today.AddDays((Convert.ToInt32(ConfigurationManager.AppSettings["AddDays"].ToString())) * -1).ToString("yyyy-MM-dd");
        public static Logger logger;

        // Add sanitization methods for logging
        private static string SanitizeLogMessage(string message)
        {
            if (string.IsNullOrEmpty(message))
                return string.Empty;

            // Remove any potential CRLF injection characters
            message = message.Replace("\r", "").Replace("\n", "");
            
            // Encode HTML to prevent XSS in log viewers
            message = HttpUtility.HtmlEncode(message);
            
            // Remove any control characters
            message = Regex.Replace(message, @"[\x00-\x1F\x7F]", "");
            
            return message;
        }

        private static string[] SanitizeLogParameters(object[] parameters)
        {
            if (parameters == null) return new string[0];
            
            return parameters.Select(p => p == null ? "null" : SanitizeLogMessage(p.ToString())).ToArray();
        }

        static void Main(string[] args)
        {
            try
            {
                if (args == null || args.Length < 2)
                {
                    throw new ArgumentException("Invalid number of arguments provided.");
                }

                string sanitizedJobArg = SanitizeLogMessage(args[0]);
                string Job = "Job_" + sanitizedJobArg;
                
                using (var scope = ScopeContext.PushProperty("Job", Job))
                {
                    logger = LogManager.GetLogger(Job);

                    // Use sanitized logging
                    logger.Info(SanitizeLogMessage("Rally-Octane synch started ...."));
                    Helpers.DisplayMessage("Rally-Octane synch [yellow]started[/]");
                    Helpers.DisplayMessage("");

                    HttpClient client = Helpers.SignIn(Helpers.OctaneUrl, "Octane");
                    if (client == null)
                    {
                        throw new InvalidOperationException("Failed to initialize HTTP client");
                    }

                    client.DefaultRequestHeaders.Add("HPECLIENTTYPE", "HPE_MQM_UI");

                    logger.Info(SanitizeLogMessage("Authentication passed...."));
                    Helpers.DisplayMessage("Authentication [green]passed[/]");

                    Helpers.dtUsers = Helpers.GetOctaneUsers(client);

                    logger.Info(SanitizeLogMessage("Got Octane users done ...."));
                    Helpers.DisplayMessage("Got Octane {0} users [green]done[/]", Helpers.dtUsers.Rows.Count);

                    // Get teams in the group
                    string[] teams = args[1].Split(';');
                    // Create a new DataTable.
                    DataTable dataTableTeams = new DataTable();
                    // Define a new column named 'team'.
                    dataTableTeams.Columns.Add("team", typeof(string));
                    // Add names to the 'team' column.
                    foreach (string team in teams)
                    {
                        if (team != "MILESTONES")
                        {
                            dataTableTeams.Rows.Add(SanitizeLogMessage(team));
                        }
                    }

                    if (args[0] == "0" && args[1] == "MILESTONES")
                    {
                        logger.Info(SanitizeLogMessage("Milestones synch started ...."));
                        Helpers.DisplayMessage("Milestones synch started");
                        Helpers.DisplayMessage("");

                        Helpers.QueryRallyMilestone = Helpers.QueryRallyMilestone.Replace("{LastUpdateDate}", "LastUpdateDate >= '" + LastUpdateDate + "'");
                        DataTable dtMilestones = Helpers.GetDataTableFromSql(Helpers.DbServer, Helpers.Database, Helpers.QueryRallyMilestone);
                        Helpers.HandleMilestone(client, dtMilestones);

                        logger.Info(SanitizeLogMessage("Milestones synch done ...."));
                        Helpers.DisplayMessage("Milestones synch done");
                        Helpers.DisplayMessage("");

                        client.Dispose();
                    }
                    else
                    {
                        string sanitizedArg = SanitizeLogMessage(args[0]);
                        logger.Info("Teams to be handled : {0}", sanitizedArg);
                        Helpers.DisplayMessage("Teams to be handled : [yellow]{0}[/]", sanitizedArg);
                        logger.Info(string.Concat(Enumerable.Repeat("*", 100)));
                        Helpers.DisplayMessage("*");

                        logger.Info(SanitizeLogMessage("Opus/Epics synch started ...."));
                        Helpers.DisplayMessage("Opus/Epics synch [yellow]started[/] ....");

                        foreach (DataRow row in dataTableTeams.Rows)
                        {
                            Helpers.DisplayMessage("*");
                            logger.Info(string.Concat(Enumerable.Repeat("*", 100)));
                            
                            string sanitizedTeam = SanitizeLogMessage(row["team"].ToString());
                            logger.Info("Tribe/Team to handle for OPUSES : {0}", sanitizedTeam);
                            Helpers.DisplayMessage("Tribe/Team to handle for OPUSES : [yellow]{0}[/]", sanitizedTeam);

                            var QueryRallyEpicWhere = "WHERE TribeName = '" + sanitizedTeam + "'";
                            DataTable dataTableEpic = Helpers.GetDataTableFromSql(Helpers.DbServer, Helpers.Database, 
                                Helpers.QueryRallyEpic + QueryRallyEpicWhere + " AND LastUpdateDate >= '" + LastUpdateDate + "'");
                            
                            logger.Info("{0} epics/opuses to handle", dataTableEpic.Rows.Count);
                            Helpers.DisplayMessage("{0} epics/opuses to handle", dataTableEpic.Rows.Count);
                            Helpers.HandleEpic(client, dataTableEpic);

                            logger.Info(string.Concat(Enumerable.Repeat("*", 100)));
                            Helpers.DisplayMessage("*");
                        }

                        logger.Info(SanitizeLogMessage("Opus/Epics synch done ...."));
                        Helpers.DisplayMessage("Opus/Epics synch [green]done[/]");

                        client.Dispose();

                        logger.Info(SanitizeLogMessage("Features synch started ...."));
                        Helpers.DisplayMessage("Features synch [yellow]started[/] ....");

                        foreach (DataRow row in dataTableTeams.Rows)
                        {
                            Helpers.DisplayMessage("*");
                            logger.Info(string.Concat(Enumerable.Repeat("*", 100)));

                            // Initialize a connection with new token for each team 
                            client = Helpers.SignIn(Helpers.OctaneUrl, "Octane");
                            if (client == null)
                            {
                                throw new InvalidOperationException("Failed to initialize HTTP client for team processing");
                            }

                            client.DefaultRequestHeaders.Add("HPECLIENTTYPE", "HPE_MQM_UI");
                            string sanitizedTeam = SanitizeLogMessage(row["team"].ToString());
                            logger.Info("Authentication for team : {0} passed...", sanitizedTeam);
                            Helpers.DisplayMessage("Authentication for team : [yellow]{0}[/] passed...", sanitizedTeam);

                            logger.Info("Tribe/Team to handle for FEATURES : {0}", sanitizedTeam);
                            Helpers.DisplayMessage("Tribe/Team to handle for FEATURES : [yellow]{0}[/]", sanitizedTeam);

                            var QueryRallyFeatureWhere = " AND FE.TribeName = '" + sanitizedTeam + "'";
                            QueryRallyStoryWhere = " AND (US.TribeName = '" + sanitizedTeam + "' OR US.OrgLVL03_Name ='" + sanitizedTeam + "')";

                            DataTable dataTableFeature = Helpers.GetDataTableFromSql(Helpers.DbServer, Helpers.Database, 
                                Helpers.QueryRallyFeature + QueryRallyFeatureWhere + " AND FE.LastUpdateDate >= '" + LastUpdateDate + "'");
                            
                            logger.Info("{0} features to handle", dataTableFeature.Rows.Count);
                            Helpers.DisplayMessage("{0} features to handle", dataTableFeature.Rows.Count);
                            Helpers.HandleFeature(client, dataTableFeature);

                            logger.Info(string.Concat(Enumerable.Repeat("*", 100)));
                            Helpers.DisplayMessage("*");
                            Helpers.DisplayMessage("Wait for 10 s...");
                            Thread.Sleep(10000);

                            client.Dispose();
                        }

                        logger.Info(SanitizeLogMessage("Features synch done ...."));
                        Helpers.DisplayMessage("Features synch [green]done[/]");

                        logger.Info(SanitizeLogMessage("UserStories without parents synch started ...."));
                        Helpers.DisplayMessage("UserStories without parents synch [yellow]started[/] ....");

                        foreach (DataRow row in dataTableTeams.Rows)
                        {
                            Helpers.DisplayMessage("*");
                            logger.Info(string.Concat(Enumerable.Repeat("*", 100)));

                            // Initialize a connection with new token for each team 
                            client = Helpers.SignIn(Helpers.OctaneUrl, "Octane");
                            if (client == null)
                            {
                                throw new InvalidOperationException("Failed to initialize HTTP client for user stories processing");
                            }

                            client.DefaultRequestHeaders.Add("HPECLIENTTYPE", "HPE_MQM_UI");

                            string sanitizedTeam = SanitizeLogMessage(row["team"].ToString());
                            QueryRallyStoryWhere = " AND FE.FormattedID is NULL AND US.TribeName = '" + sanitizedTeam + "'";

                            DataTable dataTableUserStory = Helpers.GetDataTableFromSql(Helpers.DbServer, Helpers.Database, 
                                Helpers.QueryRallyUserStory + QueryRallyStoryWhere + " AND US.LastUpdateDate >= '" + LastUpdateDate + "'");
                            
                            logger.Info("{0} userstories without parents to handle", dataTableUserStory.Rows.Count);
                            Helpers.DisplayMessage("{0} userstories without parents to handle", dataTableUserStory.Rows.Count);
                            Helpers.HandleStory_No_Parent(client, dataTableUserStory);

                            logger.Info(string.Concat(Enumerable.Repeat("*", 100)));
                            Helpers.DisplayMessage("*");

                            client.Dispose();
                        }
                        
                        logger.Info(SanitizeLogMessage("UserStories without parents synch done ...."));
                        Helpers.DisplayMessage("UserStories without parents synch [green]done[/]");

                        logger.Info(SanitizeLogMessage("Rally-Octane synch Completed ...."));
                        Helpers.DisplayMessage("");
                        Helpers.DisplayMessage("Rally-Octane synch [green]Completed[/]");
                    }
                }
            }
            catch (Exception ex)
            {
                string sanitizedMessage = SanitizeLogMessage(ex.Message);
                Helpers.DisplayMessage("An error occurred: {0}", sanitizedMessage);
                logger.Error(ex, "An error occurred: {0}", sanitizedMessage);
            }
        }
    }
}
