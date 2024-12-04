using System;
using System.Data;
using System.Net.Http;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Configuration;
using System.Security;
using System.Text.RegularExpressions;
using NLog;
using System.Web;
using System.Collections.Generic;

namespace OctaneSync
{
    public static class Octane
    {
        // Secure static fields with proper access modifiers
        private static readonly SecurityProtocol securityProtocol = new SecurityProtocol();
        public static string QueryRallyStoryWhere { get; private set; } = string.Empty;
        public static string QueryRallyStoryWhere_ORG { get; private set; } = string.Empty;
        public static readonly string LastUpdateDate = DateTime.Today
            .AddDays((Convert.ToInt32(ConfigurationManager.AppSettings["AddDays"].ToString())) * -1)
            .ToString("yyyy-MM-dd");
        private static Logger logger;

        static async Task Main(string[] args)
        {
            try
            {
                ValidateArguments(args);
                string sanitizedJob = SecurityUtility.SanitizeInput(args[0]);
                string Job = $"Job_{sanitizedJob}";

                using (var scope = ScopeContext.PushProperty("Job", Job))
                {
                    await InitializeAndExecuteSync(args, Job);
                }
            }
            catch (Exception ex)
            {
                HandleException(ex);
            }
        }

        private static void ValidateArguments(string[] args)
        {
            if (args == null || args.Length < 2)
            {
                throw new ArgumentException("Invalid number of arguments provided.");
            }

            if (!SecurityUtility.IsValidJobName(args[0]))
            {
                throw new SecurityException("Invalid job name format.");
            }
        }

        private static async Task InitializeAndExecuteSync(string[] args, string Job)
        {
            InitializeLogger(Job);
            LogStartup();

            using (HttpClient client = await InitializeHttpClient())
            {
                await ProcessUsers(client);
                DataTable dataTableTeams = CreateTeamsDataTable(args[1]);

                if (args[0] == "0" && args[1] == "MILESTONES")
                {
                    await ProcessMilestonesSync(client);
                }
                else
                {
                    await ProcessTeamsSync(client, args[0], dataTableTeams);
                }
            }
        }

        private static void InitializeLogger(string Job)
        {
            logger = LogManager.GetLogger(Job);
        }

        private static void LogStartup()
        {
            SecurityUtility.LogMessage(logger, "Rally-Octane synch started", LogLevel.Info);
            Helpers.DisplayMessage("Rally-Octane synch [yellow]started[/]");
            Helpers.DisplayMessage("");
        }

        private static async Task<HttpClient> InitializeHttpClient()
        {
            HttpClient client = Helpers.SignIn(Helpers.OctaneUrl, "Octane");
            if (client == null)
            {
                throw new SecurityException("Failed to initialize secure HTTP client.");
            }

            client.DefaultRequestHeaders.Add("HPECLIENTTYPE", "HPE_MQM_UI");
            SecurityUtility.LogMessage(logger, "Authentication passed", LogLevel.Info);
            Helpers.DisplayMessage("Authentication [green]passed[/]");

            return client;
        }

        private static async Task ProcessUsers(HttpClient client)
        {
            Helpers.dtUsers = await Helpers.GetOctaneUsersAsync(client);
            SecurityUtility.LogMessage(logger, $"Got Octane {Helpers.dtUsers.Rows.Count} users done", LogLevel.Info);
            Helpers.DisplayMessage("Got Octane {0} users [green]done[/]", Helpers.dtUsers.Rows.Count);
        }

        private static DataTable CreateTeamsDataTable(string teamsInput)
        {
            string[] teams = SecurityUtility.SanitizeAndValidateTeams(teamsInput.Split(';'));
            DataTable dataTableTeams = new DataTable();
            dataTableTeams.Columns.Add("team", typeof(string));

            foreach (string team in teams)
            {
                if (team != "MILESTONES")
                {
                    dataTableTeams.Rows.Add(SecurityUtility.SanitizeInput(team));
                }
            }

            return dataTableTeams;
        }

        private static async Task ProcessMilestonesSync(HttpClient client)
        {
            try
            {
                SecurityUtility.LogMessage(logger, "Milestones synch started", LogLevel.Info);
                Helpers.DisplayMessage("Milestones synch started");
                Helpers.DisplayMessage("");

                string secureQuery = await PrepareSecureMilestoneQuery();
                DataTable dtMilestones = await Helpers.GetDataTableFromSqlAsync(
                    Helpers.DbServer,
                    Helpers.Database,
                    secureQuery);

                await Helpers.HandleMilestoneAsync(client, dtMilestones);

                LogMilestonesCompletion();
            }
            catch (Exception ex)
            {
                HandleSyncException(ex, "milestones");
            }
        }

        private static async Task<string> PrepareSecureMilestoneQuery()
        {
            string dateCondition = SecurityUtility.SanitizeSqlParameter($"LastUpdateDate >= '{LastUpdateDate}'");
            return Helpers.QueryRallyMilestone.Replace("{LastUpdateDate}", dateCondition);
        }

        private static void LogMilestonesCompletion()
        {
            SecurityUtility.LogMessage(logger, "Milestones synch done", LogLevel.Info);
            Helpers.DisplayMessage("Milestones synch done");
            Helpers.DisplayMessage("");
        }

        private static async Task ProcessTeamsSync(HttpClient client, string jobNumber, DataTable dataTableTeams)
        {
            try
            {
                LogTeamsProcessingStart(jobNumber);
                await ProcessOpusEpicsSync(client, dataTableTeams);
                await ProcessFeaturesSync(client, dataTableTeams);
                await ProcessUserStoriesSync(client, dataTableTeams);
                LogTeamsProcessingCompletion();
            }
            catch (Exception ex)
            {
                HandleSyncException(ex, "teams");
            }
        }

        private static void LogTeamsProcessingStart(string jobNumber)
        {
            SecurityUtility.LogMessage(logger, $"Teams to be handled : {jobNumber}", LogLevel.Info);
            Helpers.DisplayMessage("Teams to be handled : [yellow]{0}[/]", jobNumber);
            SecurityUtility.LogMessage(logger, new string('*', 100), LogLevel.Info);
            Helpers.DisplayMessage("*");
        }

        private static async Task ProcessOpusEpicsSync(HttpClient client, DataTable dataTableTeams)
        {
            SecurityUtility.LogMessage(logger, "Opus/Epics synch started", LogLevel.Info);
            Helpers.DisplayMessage("Opus/Epics synch [yellow]started[/]");

            foreach (DataRow row in dataTableTeams.Rows)
            {
                await ProcessSingleTeamOpusEpics(client, row);
            }

            SecurityUtility.LogMessage(logger, "Opus/Epics synch done", LogLevel.Info);
            Helpers.DisplayMessage("Opus/Epics synch [green]done[/]");
        }

        private static async Task ProcessSingleTeamOpusEpics(HttpClient client, DataRow row)
        {
            string teamName = SecurityUtility.SanitizeInput(row["team"].ToString());
            LogTeamProcessing(teamName, "OPUSES");

            string secureQuery = await PrepareSecureEpicQuery(teamName);
            DataTable dataTableEpic = await Helpers.GetDataTableFromSqlAsync(
                Helpers.DbServer,
                Helpers.Database,
                secureQuery);

            LogEpicsCount(dataTableEpic.Rows.Count);
            await Helpers.HandleEpicAsync(client, dataTableEpic);
        }

        private static async Task<string> PrepareSecureEpicQuery(string teamName)
        {
            string whereClause = SecurityUtility.SanitizeSqlParameter($"TribeName = '{teamName}'");
            string dateCondition = SecurityUtility.SanitizeSqlParameter($"LastUpdateDate >= '{LastUpdateDate}'");
            return $"{Helpers.QueryRallyEpic} WHERE {whereClause} AND {dateCondition}";
        }

        private static void LogTeamProcessing(string teamName, string type)
        {
            Helpers.DisplayMessage("*");
            SecurityUtility.LogMessage(logger, new string('*', 100), LogLevel.Info);
            SecurityUtility.LogMessage(logger, $"Tribe/Team to handle for {type} : {teamName}", LogLevel.Info);
            Helpers.DisplayMessage($"Tribe/Team to handle for {type} : [yellow]{teamName}[/]");
        }

        private static void LogEpicsCount(int count)
        {
            Helpers.DisplayMessage("{0} epics/opuses to handle", count);
            SecurityUtility.LogMessage(logger, $"{count} epics/opuses to handle", LogLevel.Info);
        }

        private static async Task ProcessFeaturesSync(HttpClient client, DataTable dataTableTeams)
        {
            SecurityUtility.LogMessage(logger, "Features synch started", LogLevel.Info);
            Helpers.DisplayMessage("Features synch [yellow]started[/]");

            foreach (DataRow row in dataTableTeams.Rows)
            {
                await ProcessSingleTeamFeatures(client, row);
                await Task.Delay(10000); // Security delay to prevent rate limiting
            }

            SecurityUtility.LogMessage(logger, "Features synch done", LogLevel.Info);
            Helpers.DisplayMessage("Features synch [green]done[/]");
        }

        private static async Task ProcessUserStoriesSync(HttpClient client, DataTable dataTableTeams)
        {
            SecurityUtility.LogMessage(logger, "UserStories without parents synch started", LogLevel.Info);
            Helpers.DisplayMessage("UserStories without parents synch [yellow]started[/]");

            foreach (DataRow row in dataTableTeams.Rows)
            {
                await ProcessSingleTeamUserStories(client, row);
            }

            SecurityUtility.LogMessage(logger, "UserStories without parents synch done", LogLevel.Info);
            Helpers.DisplayMessage("UserStories without parents synch [green]done[/]");
        }

        private static void LogTeamsProcessingCompletion()
        {
            SecurityUtility.LogMessage(logger, "Rally-Octane synch Completed", LogLevel.Info);
            Helpers.DisplayMessage("");
            Helpers.DisplayMessage("Rally-Octane synch [green]Completed[/]");
        }

        private static void HandleException(Exception ex)
        {
            string sanitizedMessage = SecurityUtility.SanitizeExceptionMessage(ex.Message);
            SecurityUtility.LogMessage(logger, $"An error occurred: {sanitizedMessage}", LogLevel.Error);
            Helpers.DisplayMessage("An error occurred: {0}", sanitizedMessage);
        }

        private static void HandleSyncException(Exception ex, string syncType)
        {
            string sanitizedMessage = SecurityUtility.SanitizeExceptionMessage(ex.Message);
            SecurityUtility.LogMessage(logger, $"Error during {syncType} sync: {sanitizedMessage}", LogLevel.Error);
            Helpers.DisplayMessage($"Error during {syncType} sync: {{0}}", sanitizedMessage);
            throw new SecurityException($"Sync operation failed for {syncType}", ex);
        }
    }

    // SecurityUtility class definition (as provided in the previous response)
    public static class SecurityUtility
    {
        private static readonly Regex validJobNamePattern = new Regex(@"^[a-zA-Z0-9_-]+$", RegexOptions.Compiled);
        private static readonly Regex sqlInjectionPattern = new Regex(
            @"[-;]|(?:\b(?:ALTER|CREATE|DELETE|DROP|EXEC(?:UTE)?|INSERT(?:\s+INTO)?|MERGE|SELECT|UPDATE|UNION(?:\s+ALL)?)\b)", 
            RegexOptions.IgnoreCase | RegexOptions.Compiled);

        public static string SanitizeInput(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
                return string.Empty;

            return HttpUtility.HtmlEncode(input.Trim());
        }

        public static bool IsValidJobName(string jobName)
        {
            return !string.IsNullOrWhiteSpace(jobName) && validJobNamePattern.IsMatch(jobName);
        }

        public static string[] SanitizeAndValidateTeams(string[] teams)
        {
            if (teams == null)
                return new string[0];

            return teams.Select(team => SanitizeInput(team))
                       .Where(team => !string.IsNullOrWhiteSpace(team))
                       .ToArray();
        }

        public static string SanitizeSqlParameter(string parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter))
                return string.Empty;

            if (sqlInjectionPattern.IsMatch(parameter))
                throw new SecurityException("Potential SQL injection detected.");

            return parameter;
        }

        public static string SanitizeExceptionMessage(string message)
        {
            if (string.IsNullOrWhiteSpace(message))
                return "An error occurred.";

            return HttpUtility.HtmlEncode(message.Replace(Environment.NewLine, " ").Trim());
        }

        public static void LogMessage(Logger logger, string message, LogLevel level)
        {
            if (logger == null || string.IsNullOrWhiteSpace(message))
                return;

            string sanitizedMessage = SanitizeInput(message);
            
            switch (level)
            {
                case LogLevel.Info:
                    logger.Info(sanitizedMessage);
                    break;
                case LogLevel.Error:
                    logger.Error(sanitizedMessage);
                    break;
                default:
                    logger.Debug(sanitizedMessage);
                    break;
            }
        }
    }
}
