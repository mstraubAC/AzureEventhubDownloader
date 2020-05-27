using System;
using System.CommandLine;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;

namespace EventHubsDownloader
{
    class Program
    {
        private static TimeSpan maxWaitTimeForFirstEvent = new TimeSpan(0, 0, 10); // 10 seconds

        /// <summary> 
        /// Saves all data from all partitions of an eventhub consumer group
        /// </summary>
        /// <param name="ehNsConnectionString">Connection string for the EventHub Namespace</param>
        /// <param name="ehName">Name of the EventHub</param>
        /// <param name="ehConsumerGroup">The consumer group of the EventHub</param>
        /// <param name="outputFilename">The x dimension size to crop the picture. The default is 0 indicating no cropping is required.</param>

        static async Task Main(string ehNsConnectionString, string ehName, string ehConsumerGroup, FileInfo outputFilename)
        {
			Console.WriteLine($"EH-Name {ehName}");
			Console.WriteLine($"EH-Consumer Group {ehConsumerGroup}");

            EventHubConsumerClient ehClient = new EventHubConsumerClient(ehConsumerGroup, ehNsConnectionString, ehName);

            var ehPartitions = await ehClient.GetPartitionIdsAsync();
            Console.WriteLine($"Reading data from {ehPartitions.Length} partitions");

            // configure data retrieval
            var readOptions = new ReadEventOptions();
            readOptions.MaximumWaitTime = maxWaitTimeForFirstEvent;
            var ehEvents = ehClient.ReadEventsAsync(readOptions);

            // delete target file if it exists
            if (outputFilename.Exists) {
                outputFilename.Delete();
            }
                
            // grab data
            using (var outStream = new FileStream(outputFilename.FullName, FileMode.Append)) { 
                await foreach (PartitionEvent ehEvent in ehEvents)
                {
                    byte[] data = ehEvent.Data.Body.ToArray();
                    Console.WriteLine($"Retrieved {data.Length} bytes");
                    outStream.Write(data, 0, data.Length);
                }
            }
        }
    }
}
