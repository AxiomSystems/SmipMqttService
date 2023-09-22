using System;
using System.Runtime.InteropServices;
using MQTTnet;
using MQTTnet.Client;
using System.Threading.Tasks;
using System.Threading;
using MQTTnet.Protocol;
using System.Text;
using System.Collections.Generic;
using System.Text.Json;
using System.IO;
using System.Reflection.PortableExecutable;

namespace SmipMqttService
{
    internal class MqttService
    {
        static string dataRoot = "";
        static string histRoot = "MqttHist";
        static string topicListFile = "MqttTopicList.txt";
        static string topicSubscriptionFile = "CloudAcquiredTagList.txt";
        static string compoundSeperator = @"/:/";
        static List<string> knownTopics = new List<string>();
        static async Task Main(string[] args)
        {
            
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                dataRoot = @"C:\ProgramData\ThinkIQ\DataRoot";
                Console.WriteLine("Starting MQTT Helper Service on Windows!\r\nUsing root: " + dataRoot);
            } else
            {
                dataRoot = "/opt/thinkiq/DataRoot";
                Console.WriteLine("Starting MQTT Helper Service on *nix!\r\nUsing root: " + dataRoot);
            }

            // Setup files and folders we need
            histRoot = Path.Combine(dataRoot, histRoot);
            Directory.CreateDirectory(histRoot);

            //TODO: This should come from a config file
            string broker = "192.168.10.5";
            int port = 1883;
            string clientId = Guid.NewGuid().ToString();
            string topic = "#";
            string username = "";
            string password = "";

            var factory = new MqttFactory();
            var mqttClient = factory.CreateMqttClient();

            var options = new MqttClientOptionsBuilder()
                .WithTcpServer(broker, port) // MQTT broker address and port
                .WithCredentials(username, password) // Set username and password
                .WithClientId(clientId)
                .WithCleanSession()
                .Build();
            var connectResult = await mqttClient.ConnectAsync(options);

            if (connectResult.ResultCode == MqttClientConnectResultCode.Success)
            {
                Console.WriteLine("Connected to MQTT broker successfully.");
                await mqttClient.SubscribeAsync(topic);
                mqttClient.ApplicationMessageReceivedAsync += e =>
                {
                    Console.WriteLine("Incoming message: " + Newtonsoft.Json.JsonConvert.SerializeObject(e));
                    if (CheckIfTopicSubscribed(e.ApplicationMessage.Topic))
                    {
                        CacheMessageValue(e.ApplicationMessage.Topic, Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment));
                    }
                    LearnTopicsFromTopic(e.ApplicationMessage.Topic, Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment));
                    UpdateTopicCache();
                    return Task.CompletedTask;
                };

                while (true) {
                    await Task.Delay(1000);
                }

                // Unsubscribe and disconnect
                await mqttClient.UnsubscribeAsync(topic);
                await mqttClient.DisconnectAsync();
            }
            else
            {
                Console.WriteLine($"Failed to connect to MQTT broker: {connectResult.ResultCode}");
            }
        }

        private static bool CheckIfTopicSubscribed(string incomingTopic)
        {
            // Since the Connector might lock this file, we have to StreamReader it
            using (var fs = new FileStream(Path.Combine(dataRoot, topicSubscriptionFile), FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var sr = new StreamReader(fs, Encoding.Default))
            {
                while (!sr.EndOfStream)
                {
                    var unCompoundTopic = sr.ReadLine();
                    if (unCompoundTopic.IndexOf(compoundSeperator) > -1)
                    {
                        unCompoundTopic = unCompoundTopic.Substring(0, unCompoundTopic.IndexOf(compoundSeperator));
                    }
                    if (incomingTopic == unCompoundTopic)
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        private static void CacheMessageValue(string incomingTopic, string incomingMessage)
        {
            var cachePath = Path.Combine(histRoot, (Base64Encode(incomingTopic) + ".txt"));
            File.WriteAllText(cachePath, incomingMessage);
        }

        private static void LearnTopicsFromTopic(string incomingTopic, string incomingMessage, string seperator = "/")
        {
            if (!knownTopics.Contains(incomingTopic))
            {
                knownTopics.Add(incomingTopic);
            }
            if (incomingMessage != String.Empty)
            {
                if (IsValidJson(incomingMessage))
                {
                    var options = new JsonDocumentOptions
                    {
                        AllowTrailingCommas = true,
                        CommentHandling = JsonCommentHandling.Skip
                    };
                    using (JsonDocument document = JsonDocument.Parse(incomingMessage, options))
                    {
                        LearnTopicsFromPayload(incomingTopic, incomingMessage, compoundSeperator);
                    }
                }
            }
        }

        private static void LearnTopicsFromPayload(string incomingTopic, string incomingMessage, string seperator="/")
        {
            if (incomingMessage != String.Empty)
            {
                if (IsValidJson(incomingMessage))
                {
                    var options = new JsonDocumentOptions
                    {
                        AllowTrailingCommas = true,
                        CommentHandling = JsonCommentHandling.Skip
                    };
                    try
                    {
                        using (JsonDocument document = JsonDocument.Parse(incomingMessage, options))
                        {
                            foreach (JsonProperty property in document.RootElement.EnumerateObject())
                            {
                                if (property.Value.ValueKind == JsonValueKind.Object)
                                {
                                    LearnTopicsFromPayload(incomingTopic + seperator + property.Name, JsonSerializer.Serialize(property.Value));
                                }
                                else
                                {
                                    LearnTopicsFromTopic(incomingTopic + seperator + property.Name, String.Empty);
                                }

                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        //Maybe it wasn't valid JSON after all
                    }
                }
            }
        }

        private static void UpdateTopicCache()
        {
            Console.WriteLine("Topic list now: " + JsonSerializer.Serialize(knownTopics));
            //StreamReaders seem safer when multiple things might be accessing the file
            using (var sw = new StreamWriter(Path.Combine(dataRoot, topicListFile), false))
            {
                foreach (var topic in knownTopics)
                {
                    sw.WriteLine(topic);
                }
            }
        }
        private static bool IsValidJson(string json)
        {
            //TODO: This returns true for simple strings for some reason
            if (json == null || json == String.Empty)
                return false;
            try
            {
                JsonDocument.Parse(json);
                return true;
            }
            catch (JsonException)
            {
                return false;
            }
        }

        private static string Base64Encode(string plainText)
        {
            var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(plainText);
            return System.Convert.ToBase64String(plainTextBytes);
        }

        private static string Base64Decode(string base64EncodedData)
        {
            var base64EncodedBytes = System.Convert.FromBase64String(base64EncodedData);
            return System.Text.Encoding.UTF8.GetString(base64EncodedBytes);
        }
    }
}
