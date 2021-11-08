using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DeliveryOrderProcessor
{
    public static class DeliveryOrderProcessor
    {
        [FunctionName("DeliveryOrderProcessor")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req)
        {
            var endpointUri = Environment.GetEnvironmentVariable("CosmosDbEndpointUri");
            var primaryKey = Environment.GetEnvironmentVariable("CosmosDbPrimaryKey");
            var cosmosClient = new CosmosClient(endpointUri, primaryKey, new CosmosClientOptions() { ApplicationName = "Orders" });

            Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync("Orders");
            Container container = await database.CreateContainerIfNotExistsAsync("Orders", "/partitionKey");

            var orderString = await req.ReadAsStringAsync();
            var order = JsonConvert.DeserializeObject<IDictionary<string, object>>(orderString);
            var id = Guid.NewGuid().ToString();
            var partitionKey = "Orders";

            order["id"] = id;
            order["partitionKey"] = partitionKey;

            var record = new { Data = order, id = id, partitionKey = partitionKey };

            await container.CreateItemAsync(record, new PartitionKey(record.partitionKey));

            return new OkObjectResult("Success");
        }
    }
}
