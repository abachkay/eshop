using Azure.Storage.Blobs;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace OrdersItemsReceiver
{
    public static class OrderItemsReserver
    {
        [FunctionName("OrderItemsReserver")]
        public static async Task Run([ServiceBusTrigger("orders", Connection = "ServiceBusConnectionKey")] string myQueueItem)
        {
            var retryCount = 3;
            var hasFailed = false;

            do
            {
                try
                {
                    var blobServiceClient = new BlobServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
                    var containerClient = blobServiceClient.GetBlobContainerClient("items-orders");

                    await containerClient.CreateIfNotExistsAsync();

                    BlobClient blobClient = containerClient.GetBlobClient($"{Guid.NewGuid()}.json");

                    using (MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes(myQueueItem)))
                    {
                        await blobClient.UploadAsync(ms);
                    }
                }
                catch
                {
                    retryCount--;
                    hasFailed = true;
                }
            }
            // Try 3 times to save order details into the Blob Storage.
            while (retryCount > 0 && hasFailed);

            // If saving blob failed 3 times, then send message to Service Bus errors queue. Logic app will use it to send email later.
            if (retryCount == 0)
            {
                var errorDetails = $"Order processing failed 3 times, details: {myQueueItem}";
                var serviceBusConnectionString = Environment.GetEnvironmentVariable("ServiceBusConnectionKey");
                var serviceBusQueueName = Environment.GetEnvironmentVariable("ErrorsQueueName");
                var queueClient = new QueueClient(serviceBusConnectionString, serviceBusQueueName);
                var message = new Message(Encoding.UTF8.GetBytes(errorDetails));

                await queueClient.SendAsync(message);
                await queueClient.CloseAsync();
            }
        }
    }
}
