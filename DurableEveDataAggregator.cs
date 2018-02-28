using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Logging;
using Microsoft.WindowsAzure.Storage.Blob;

namespace DurableEveDataAggregator
{
    public static class DurableEveDataAggregator
    {
        [StorageAccount("EveDataStore")]
        [FunctionName("DurableEveDataAggregationEntryPoint")]
        public static async Task DurableEveDataAggregationEntryPoint([BlobTrigger("eve-data-temp/{name}")]CloudBlockBlob myBlob, string name, [OrchestrationClient] DurableOrchestrationClient starter,
            TraceWriter log)
        {
            string instanceId = await starter.StartNewAsync("EveDurableOrchestrator", name);

            log.Info($"Started orchestration with ID = '{instanceId}'");
        }

        [FunctionName("EveDurableOrchestrator")]
        public static async Task<int> Run([OrchestrationTrigger] DurableOrchestrationContext dataAggregatorContext, TraceWriter log, [Blob("eve-data-temp")] CloudBlobContainer container)
        {
            var blobName = dataAggregatorContext.GetInput<string>();
            var blobReference = container.GetBlockBlobReference(blobName);
            var dataStream = new MemoryStream();

            log.Info($"Downloading of the file '{blobName}'...");

            await blobReference.DownloadToStreamAsync(dataStream);
            
            log.Info($"Downloading of the file '{blobName}'. Completed.");
            log.Info($"The file size in stream: '{dataStream.Length}'");

            //ToDo: do splitting the blob file to portions (should be sertializable collections) and fan out to activity function
            //Probably that is not good idea to download a big blob here :)

            var results = new Task<int>[10];

            for (int i = 0; i < 10; i++)
            {
                results[i] = dataAggregatorContext.CallActivityAsync<int>("EveProcessRowsActivity", "somedata");

            }

            await Task.WhenAll(results);

            int total = results.Sum(t => t.Result);

            return total;
        }

        [FunctionName("EveProcessRowsActivity")]
        public static int EveProcessRowsActivity([ActivityTrigger] string someData, TraceWriter log)
        {
            log.Info($"Started. {someData}");

            return 5;
        }
    }
}
