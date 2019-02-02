using Hyak.Common;
using Microsoft.Azure;
using Microsoft.Azure.Common.Authentication;
using Microsoft.Azure.Common.Authentication.Factories;
using Microsoft.Azure.Common.Authentication.Models;
using Microsoft.Azure.Management.HDInsight;
using Microsoft.Azure.Management.HDInsight.Job;
using Microsoft.Azure.Management.HDInsight.Job.Models;
using Microsoft.Azure.Management.Resources;
using Microsoft.Rest;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security;
using System.Text;
using System.Threading;

namespace HDInsights
{
    class Program
    {
        private static string user = "";
        private static string password = "";

        private static string storageAccount = "";
        private static string storageKey = "";
        private static string storageContainer = "";

        private static Guid subId = new Guid("");
        private static string tenantId = "";
        private static string appId = "";
        private static string secretKey = "";

        static void Main(string[] args)
        {
            var key = new SecureString();
            foreach (char c in secretKey) { key.AppendChar(c); }

            var tokenCreds = GetTokenCloudCredentials(tenantId, appId, key);

            var resourceManagementClient = new ResourceManagementClient(new TokenCloudCredentials(subId.ToString(),tokenCreds.Token));
            resourceManagementClient.Providers.Register("Microsoft.HDInsight");

            var hdiManagementClient = new HDInsightManagementClient(new TokenCredentials(tokenCreds.Token));
            hdiManagementClient.SubscriptionId = subId.ToString();

            var clusterName = GetClusterName(hdiManagementClient);

            Console.ReadLine();

            var hdiJobManagementClient = new HDInsightJobManagementClient(clusterName + ".azurehdinsight.net",
                new BasicAuthenticationCloudCredentials
                {
                    Username = user,
                    Password = password
                });

            Dictionary<string, string> defines = new Dictionary<string, string> { { "hive.execution.engine", "tez" }, { "hive.exec.reducers.max", "1" } };
            List<string> hadoopArgs = new List<string> { { "argA" }, { "argB" } };
            var job = new HiveJobSubmissionParameters
            {
                Query = "SHOW TABLES",
                Defines = defines,
                Arguments = hadoopArgs
            };

            SubmitJobToCluster(hdiJobManagementClient, job);

            Console.ReadLine();
        }

        /// Get the access token for a service principal and provided key.          
        public static TokenCloudCredentials GetTokenCloudCredentials(string tenantId, string clientId, SecureString secretKey)
        {
            var authFactory = new AuthenticationFactory();
            var account = new AzureAccount { Type = AzureAccount.AccountType.ServicePrincipal, Id = clientId };
            var env = AzureEnvironment.PublicEnvironments[EnvironmentName.AzureCloud];
            var accessToken =
                authFactory.Authenticate(account, env, tenantId, secretKey, ShowDialog.Never).AccessToken;

            return new TokenCloudCredentials(accessToken);
        }

        public static string GetClusterName(HDInsightManagementClient hdiManagementClient)
        {
            var clusters = hdiManagementClient.Clusters.List();
            foreach (var cluster in clusters)
            {
                Console.WriteLine("Cluster Name: " + cluster.Name);
                Console.WriteLine("\t Cluster type: " + cluster.Properties.ClusterDefinition.Kind);
                Console.WriteLine("\t Cluster location: " + cluster.Location);
                Console.WriteLine("\t Cluster version: " + cluster.Properties.ClusterVersion);
            }
            Console.WriteLine("Press Enter to continue");

            return clusters.First().Name;
        }

        public static void SubmitJobToCluster(HDInsightJobManagementClient hdiJobManagementClient, HiveJobSubmissionParameters job)
        {
            System.Console.WriteLine("Submitting the Hive job to the cluster...");
            var jobResponse = hdiJobManagementClient.JobManagement.SubmitHiveJob(job);
            var jobId = jobResponse.JobSubmissionJsonResponse.Id;
            System.Console.WriteLine("Response status code is " + jobResponse.StatusCode);
            System.Console.WriteLine("JobId is " + jobId);

            System.Console.WriteLine("Waiting for the job completion ...");

            // Wait for job completion
            var jobDetail = hdiJobManagementClient.JobManagement.GetJob(jobId).JobDetail;
            while (!jobDetail.Status.JobComplete)
            {
                Thread.Sleep(1000);
                jobDetail = hdiJobManagementClient.JobManagement.GetJob(jobId).JobDetail;
            }

            // Get job output
            var storageAccess = new AzureStorageAccess(storageAccount, storageKey,
                storageContainer);
            var output = (jobDetail.ExitValue == 0)
                ? hdiJobManagementClient.JobManagement.GetJobOutput(jobId, storageAccess) // fetch stdout output in case of success
                : hdiJobManagementClient.JobManagement.GetJobErrorLogs(jobId, storageAccess); // fetch stderr output in case of failure

            System.Console.WriteLine("Job output is: ");

            using (var reader = new StreamReader(output, Encoding.UTF8))
            {
                string value = reader.ReadToEnd();
                System.Console.WriteLine(value);
            }
        }
    }
}
