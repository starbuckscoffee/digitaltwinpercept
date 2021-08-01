using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Azure.Identity;
using Azure.DigitalTwins.Core;
using Azure;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Samples.AdtIothub
{
    public static class DeviceTelemetryToTwinFunc
    {

        private static string adtInstanceUrl = "https://hgadt01.api.sea.digitaltwins.azure.net";
        //private static string adtAppId = "https://digitaltwins.azure.net";

        // To Do httpclient...

        [FunctionName("DeviceTelemetryToTwinFunc")]
        public static async Task Run([EventHubTrigger("deviceevents", Connection = "EVENTHUB_CONNECTIONSTRING")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            // Create Digital Twin Client
            var credential = new DefaultAzureCredential();
            var client = new DigitalTwinsClient( new Uri(adtInstanceUrl), credential);

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    // Replace these two lines with your processing logic.
                    var deviceMessage = (dynamic)JsonConvert.DeserializeObject(messageBody);
                    // foreach loop
                    foreach ( var myidx in deviceMessage["NEURAL_NETWORK"])
                    {
                        //Debugging purpose
                        //   Console.WriteLine($"myidx: {myidx}");
                        string tmp = $"{myidx}";
                        var nnMessage = (dynamic)JsonConvert.DeserializeObject(tmp);
                        //Debugging purpose
                        //   string nnMessageLabel = nnMessage["label"];
                        //   Console.WriteLine($"{nnMessageLabel}"); 
                        string label = nnMessage["label"].ToString();
                        string confidence = nnMessage["confidence"].ToString();
                        string timestamp = nnMessage["timestamp"].ToString();
                        if (nnMessage["label"]=="teddy bear"){
                            var updateTwinData = new JsonPatchDocument();
                            updateTwinData.AppendAdd("/Label", label);
                            updateTwinData.AppendAdd("/Confidence", confidence );
                            updateTwinData.AppendAdd("/timestamp", timestamp);
                            await client.UpdateDigitalTwinAsync("PerceptSiteFloor", updateTwinData);
                            string updateTwinDataStr = updateTwinData.ToString(); 
                            log.LogInformation($"Update Device PerceptSiteFloor with {updateTwinDataStr} at {DateTime.Now.ToString()}");
                        }
                    }

                    //log.LogInformation($"C# Event Hub trigger function processed a message: {messageBody}");
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
