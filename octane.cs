
using System;
using System.Data;
using System.Net.Http;
using System.Linq;
using System.Threading;
using System.Configuration;
using NLog;

namespace OctaneSync
{
    public static class Octane
    {
        public static string QueryRallyStoryWhere = string.Empty;
        public static string QueryRallyStoryWhere_ORG = string.Empty;
        public static readonly string LastUpdateDate = DateTime.Today.AddDays((Convert.ToInt32(ConfigurationManager.AppSettings["AddDays"].ToString())) * -1).ToString("yyyy-MM-dd");
        public static Logger logger; //= LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            try
            {
                    string Job = "Job_" + args[0];
                    using (var scope = ScopeContext.PushProperty("Job", Job))
                    {

                        logger = LogManager.GetLogger(Job);

                        Helpers.DisplayMessage("Rally-Octane synch [yellow]started[/]");
                        Helpers.DisplayMessage("");
                        logger.Info("Rally-Octane synch started ....");

                        HttpClient client = Helpers.SignIn(Helpers.OctaneUrl,"Octane");
                        client.DefaultRequestHeaders.Add("HPECLIENTTYPE", "HPE_MQM_UI");

                        logger.Info("Authentication passed....");

                        Helpers.DisplayMessage("Authentication [green]passed[/]");

                        Helpers.dtUsers = Helpers.GetOctaneUsers(client);

                        logger.Info("Got Octane users done ....");
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
                                dataTableTeams.Rows.Add(team);
                            }
                        }

                        if (args[0] == "0" && args[1] == "MILESTONES")
                        {
                            Helpers.DisplayMessage("Milestones synch started");
                            Helpers.DisplayMessage("");
                            logger.Info("Milestones synch started ....");

                            Helpers.QueryRallyMilestone = Helpers.QueryRallyMilestone.Replace("{LastUpdateDate}", "LastUpdateDate >= '" + LastUpdateDate + "'");
                            DataTable dtMilestones = Helpers.GetDataTableFromSql(Helpers.DbServer, Helpers.Database, Helpers.QueryRallyMilestone);
                            Helpers.HandleMilestone(client, dtMilestones);
                            //DataTable temp = Helpers.GetMilestones(client);

                            Helpers.DisplayMessage("Milestones synch done");
                            Helpers.DisplayMessage("");
                            logger.Info("Milestones synch done ....");

                            client.Dispose();
                        }
                        else
                        {
                            Octane.logger.Info("Teams to be handled : {0}", args[0]);
                            Helpers.DisplayMessage("Teams to be handled : [yellow]{0}[/]", args[0]);
                            Octane.logger.Info(string.Concat(Enumerable.Repeat("*", 100)));
                            Helpers.DisplayMessage("*");

                            Octane.logger.Info("Opus/Epics synch started ....");

                            Helpers.DisplayMessage("Opus/Epics synch [yellow]started[/] ....");


                            foreach (DataRow row in dataTableTeams.Rows)
                            {
                                Helpers.DisplayMessage("*");
                                Octane.logger.Info(string.Concat(Enumerable.Repeat("*", 100)));
                                Octane.logger.Info("Tribe/Team to handle for OPUSES : {0}", row["team"].ToString());
                                Helpers.DisplayMessage("Tribe/Team to handle for OPUSES : [yellow]{0}[/]", row["team"].ToString());

                                var QueryRallyEpicWhere = "WHERE TribeName = '" + row["team"].ToString() + "'";
                                DataTable dataTableEpic = Helpers.GetDataTableFromSql(Helpers.DbServer, Helpers.Database, Helpers.QueryRallyEpic + QueryRallyEpicWhere + " AND LastUpdateDate >= '" + LastUpdateDate + "'");
                                Helpers.DisplayMessage("{0} epics/opuses to handle", dataTableEpic.Rows.Count);
                                Octane.logger.Info("{0} epics/opuses to handle", dataTableEpic.Rows.Count);
                                Helpers.HandleEpic(client, dataTableEpic);

                                Octane.logger.Info(string.Concat(Enumerable.Repeat("*", 100)));
                                Helpers.DisplayMessage("*");
                            }

                            Octane.logger.Info("Opus/Epics synch done ....");
                            Helpers.DisplayMessage("Opus/Epics synch [green]done[/]");

                            client.Dispose();

                            Octane.logger.Info("Features synch started ....");
                            Helpers.DisplayMessage("Features synch [yellow]started[/] ....");

                            foreach (DataRow row in dataTableTeams.Rows)
                            {
                                Helpers.DisplayMessage("*");
                                Octane.logger.Info(string.Concat(Enumerable.Repeat("*", 100)));

                                // Initialize a connection with new token for each team 
                                client = Helpers.SignIn(Helpers.OctaneUrl, "Octane");
                                client.DefaultRequestHeaders.Add("HPECLIENTTYPE", "HPE_MQM_UI");
                                Helpers.DisplayMessage("Authentication for team : [yellow]{0}[/] passed...", row["team"].ToString());
                                Octane.logger.Info("Authentication for team : {0} passed...", row["team"].ToString());

                                Octane.logger.Info("Tribe/Team to handle for FEATURES : {0}", row["team"].ToString());
                                Helpers.DisplayMessage("Tribe/Team to handle for FEATURES : [yellow]{0}[/]", row["team"].ToString());

                                var QueryRallyFeatureWhere = " AND FE.TribeName = '" + row["team"].ToString() + "'";

                                QueryRallyStoryWhere = " AND (US.TribeName = '" + row["team"].ToString() + "' OR US.OrgLVL03_Name ='" + row["team"].ToString() + "')";

                                DataTable dataTableFeature = Helpers.GetDataTableFromSql(Helpers.DbServer, Helpers.Database, Helpers.QueryRallyFeature + QueryRallyFeatureWhere + " AND FE.LastUpdateDate >= '" + LastUpdateDate + "'");
                                Helpers.DisplayMessage("{0} features to handle", dataTableFeature.Rows.Count);
                                Octane.logger.Info("{0} features to handle", dataTableFeature.Rows.Count);
                                Helpers.HandleFeature(client, dataTableFeature);

                                Octane.logger.Info(string.Concat(Enumerable.Repeat("*", 100)));
                                Helpers.DisplayMessage("*");
                                Helpers.DisplayMessage("Wait for 10 s...");
                                Thread.Sleep(10000);

                                client.Dispose();
                            }

                            Octane.logger.Info("Features synch done ....");
                            Helpers.DisplayMessage("Features synch [green]done[/]");

                            Octane.logger.Info("UserStories without parents synch started ....");
                            Helpers.DisplayMessage("UserStories without parents synch [yellow]started[/] ....");

                            foreach (DataRow row in dataTableTeams.Rows)
                            {
                                Helpers.DisplayMessage("*");
                                Octane.logger.Info(string.Concat(Enumerable.Repeat("*", 100)));

                                // Initialize a connection with new token for each team 
                                client = Helpers.SignIn(Helpers.OctaneUrl, "Octane");
                                client.DefaultRequestHeaders.Add("HPECLIENTTYPE", "HPE_MQM_UI");

                                QueryRallyStoryWhere = " AND FE.FormattedID is NULL AND US.TribeName = '" + row["team"].ToString() + "'";

                                DataTable dataTableUserStory = Helpers.GetDataTableFromSql(Helpers.DbServer, Helpers.Database, Helpers.QueryRallyUserStory + QueryRallyStoryWhere + " AND US.LastUpdateDate >= '" + LastUpdateDate + "'");
                                Helpers.DisplayMessage("{0} userstories without parents to handle", dataTableUserStory.Rows.Count);
                                Octane.logger.Info("{0} userstories without parents to handle", dataTableUserStory.Rows.Count);
                                Helpers.HandleStory_No_Parent(client, dataTableUserStory);

                                Octane.logger.Info(string.Concat(Enumerable.Repeat("*", 100)));
                                Helpers.DisplayMessage("*");
                                //Helpers.DisplayMessage("Wait for 20 s...");
                                //Thread.Sleep(20000);

                                client.Dispose();
                            }
                            Octane.logger.Info("UserStories without parents synch done ....");
                            Helpers.DisplayMessage("UserStories without parents synch [green]done[/]");

                            Octane.logger.Info("Rally-Octane synch Completed ....");
                            Helpers.DisplayMessage("");
                            Helpers.DisplayMessage("Rally-Octane synch [green]Completed[/]");
                        }
                    }
            }
            catch (Exception ex)
            {
                Helpers.DisplayMessage("An error occurred: {0}", ex.Message);
                Octane.logger.Error(ex, "An error occurred: {0}");
            }
        }
    }
}
