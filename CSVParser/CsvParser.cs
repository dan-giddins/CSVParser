using Microsoft.AspNetCore.Mvc;
using Microsoft.WindowsAzure.Storage.Table;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.IO;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Linq;

namespace CsvParser
{
    public static class CsvParser
    {
        [FunctionName("CsvParser")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            // set account information
            // create the table client
            // get a reference to the table
            var postcodeTable = new CloudStorageAccount(
                new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(
                    Environment.GetEnvironmentVariable("PostcodeTableAccountName"),
                    Environment.GetEnvironmentVariable("PostcodeTableKey")),
                true)
                .CreateCloudTableClient()
                .GetTableReference("postcodeTable");
            // drop all rows from exsiting table
            var allRecords = (await postcodeTable.ExecuteQuerySegmentedAsync(
                new TableQuery<PostcodeRecord>().Where(
                    TableQuery.GenerateFilterCondition(
                        "PartitionKey",
                        QueryComparisons.Equal,
                        "PostcodeData")),
                    null)).Results;
            var batchDeleteOperation = new TableBatchOperation();
            if (allRecords.Count > 0)
            {
                foreach (var recordChunk in allRecords
                    .Select((x, i) => new { Index = i, Value = x })
                    .GroupBy(x => x.Index / 100)
                    .Select(x => x.Select(v => v.Value).ToList())
                    .ToList())
                {
                    foreach (var record in recordChunk)
                    {
                        batchDeleteOperation.Delete(record);
                    }
                    await postcodeTable.ExecuteBatchAsync(batchDeleteOperation);
                }
            }
            var sr = new StreamReader(req.Body);
            // discard header
            sr.ReadLine();
            var line = sr.ReadLine();
            var counter = 0;
            while (!(line is null))
            {
                var lineArray = line.Split(',');
                var postcode = lineArray[0].Replace(" ", string.Empty).ToUpper();
                log.LogInformation($"Adding Postcode {postcode} ({counter})");
                await postcodeTable.ExecuteAsync(
                    TableOperation.Insert(
                        new PostcodeRecord()
                        {
                            PartitionKey = "PostcodeData",
                            RowKey = postcode,
                            Flag = lineArray[1] == "1"
                        }));
                counter++;
                line = sr.ReadLine();
            }
            return new OkObjectResult($"Added {counter} postcodes");
        }

        public class PostcodeRecord : TableEntity
        {
            public bool Flag { get; set; }
        }
    }
}
