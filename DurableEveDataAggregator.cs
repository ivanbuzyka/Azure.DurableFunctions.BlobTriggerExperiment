using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace DurableEveDataAggregator
{
    public static class DurableEveDataAggregator
    {
        [StorageAccount("EveDataStore")]
        [FunctionName("DurableEveDataAggregationEntryPoint")]
        public static async Task DurableEveDataAggregationEntryPoint([BlobTrigger("eve-data-temp/{name}")]Stream myBlob, string name, [OrchestrationClient] DurableOrchestrationClient starter,
            TraceWriter log)
        {
            string instanceId = await starter.StartNewAsync("EveDurableOrchestrator", myBlob);

            log.Info($"Started orchestration with ID = '{instanceId}'");
        }

        [FunctionName("EveDurableOrchestrator")]
        public static async Task<int> Run([OrchestrationTrigger] DurableOrchestrationContext dataAggregatorContext, TraceWriter log)
        {
            var stream = dataAggregatorContext.GetInput<Stream>();

            log.Info($"Input stream length = '{stream.Length}'");

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
