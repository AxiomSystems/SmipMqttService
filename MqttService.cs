using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Threading;
using System.Text;
using System.Collections.Generic;
using System.Text.Json;
using System.IO;
using MQTTnet;
using MQTTnet.Client;
using Microsoft.Extensions.Configuration;
using Serilog;
using Newtonsoft.Json.Linq;
using System.Data;

namespace SmipMqttService
{
    internal class MqttService
    {
        static string dataRoot = "";
        static string logPath = "";
        static string iotIdPath = "";
        static string iotId;
        static string histRoot = "MqttHist";
        static int heartbeatSeconds = 5;
        static string heartbeatTopic = "Smip.Mqtt.Connector.Heartbeat";
        static string iotIdTopic = "Smip.Mqtt.Connector.IotId";
        static string topicListFile = "MqttTopicList.txt";
        static string topicSubscriptionFile = "CloudAcquiredTagList.txt";
        static string compoundSeperator = @"/:/";
        static List<string> knownTopics = new List<string>();
        static MqttFactory mqttFactory = new MqttFactory();
        static IMqttClient mqttClient = mqttFactory.CreateMqttClient();
        static async Task Main(string[] args)
        {

            // Figure out environment
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                dataRoot = @"C:\ProgramData\ThinkIQ\DataRoot";
                logPath = @"C:\ProgramData\ThinkIQ\DataRoot\Logs\SmipMqttLog.txt";
                iotIdPath = @"C:\ProgramData\iot-registry\.iotid";
                Console.WriteLine("Starting MQTT Helper Service on Windows!");
            }
            else
            {
                dataRoot = @"/opt/thinkiq/DataRoot";
                logPath = @"/opt/thinkiq/services/SmipMqttService/Logs/SmipMqttLog.txt";
                iotIdPath = @"/opt/iot-registry/.iotid";
                Console.WriteLine("Starting MQTT Helper Service on *nix!");
            }

            // Setup files and folders we need
            histRoot = Path.Combine(dataRoot, histRoot);
            Directory.CreateDirectory(histRoot);
            Environment.SetEnvironmentVariable("LOGFILEPATH", logPath);

            // Load config
            IConfiguration configuration = new ConfigurationBuilder()
               .AddJsonFile("appsettings.json", true, true)
               .AddEnvironmentVariables()
               .Build();
            var smipConfig = configuration.GetSection("Smip");
            int.TryParse(smipConfig.GetSection("heartbeatSeconds").Value, out heartbeatSeconds);

            // Setup Logger (using config)
            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(configuration)
                .CreateLogger();

            // Load identity (if present)
            iotId = CheckForIotId(iotIdPath);

            // Seed Topic file (if necessary)
            if (iotId != String.Empty && !CheckTopicCache(iotIdTopic))
            {
                knownTopics.Add(iotIdTopic);
                UpdateTopicCache();
            }
            if (!CheckTopicCache(heartbeatTopic))
            {
                knownTopics.Add(heartbeatTopic);
                UpdateTopicCache();
            }

            Log.Information("SMIP MQTT Service Started with Data Root: " + dataRoot);
            Log.Information("Logging to: " + logPath);

            // Setup Mqtt Connection (using config)
            var mqttConfig = configuration.GetSection("Mqtt");
            string broker = mqttConfig.GetSection("brokerHost").Value;
            int port;
            if (!int.TryParse(mqttConfig.GetSection("brokerPort").Value, out port))
            {
                port = 1883;
            }
            string clientId = "smipgw-" + Guid.NewGuid().ToString();
            var options = new MqttClientOptionsBuilder()
                .WithTcpServer(broker, port) // MQTT broker address and port
                .WithCredentials(mqttConfig.GetSection("brokerUser").Value, mqttConfig.GetSection("brokerPass").Value) // Set username and password from appsettings
                .WithClientId(clientId)
                .WithCleanSession()
                .Build();
            var connectResult = await mqttClient.ConnectAsync(options);

            // Connect and listen to broker
            if (connectResult.ResultCode == MqttClientConnectResultCode.Success)
            {
                Log.Information("Connected to MQTT broker: " + broker);
                await mqttClient.SubscribeAsync("#");
                mqttClient.ApplicationMessageReceivedAsync += e =>
                {
                    Log.Information("Incoming message for topic: " + e.ApplicationMessage.Topic);
                    Log.Debug("Payload: " + Newtonsoft.Json.JsonConvert.SerializeObject(e));
                    if (CheckIfTopicSubscribed(e.ApplicationMessage.Topic))
                    {
                        CacheMessageValue(e.ApplicationMessage.Topic, Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment));
                    }
                    LearnTopicsFromTopic(e.ApplicationMessage.Topic, Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment));
                    UpdateTopicCache();
                    return Task.CompletedTask;
                };

                while (true) {
                    Log.Debug("Sending heartbeat to broker.");
                    await SendHeartbeat(heartbeatTopic, DateTime.Now.Ticks);
                    await Task.Delay(heartbeatSeconds * 1000);
                }
            }
            else
            {
                Log.Fatal($"Failed to connect to MQTT broker: {connectResult.ResultCode}");
                Environment.Exit(1);
            }
        }

        static void CurrentDomain_ProcessExit(object sender, EventArgs e)
        {
            // Unsubscribe and disconnect
            _ = mqttClient.UnsubscribeAsync("#");
            _ = mqttClient.DisconnectAsync();
            Log.Information("SMIP MQTT Service shutting down cleanly");
        }

        static string CheckForIotId(string iotIdPath)
        {
            if (File.Exists(iotIdPath))
            {
                try
                {
                    iotId = File.ReadAllText(iotIdPath);
                    Log.Information("Using IOT ID: " + iotId);
                    return iotId;
                }
                catch (Exception ex)
                {
                    Log.Warning("Found .iotid file, but could not read for publication.");
                    Log.Debug(ex.Message);
                    return String.Empty;
                }
            }
            else
            {
                Log.Debug("No .iotid found at " + iotIdPath);
                return String.Empty;
            }
        }

        static async Task SendHeartbeat(string topic, long value)
        {
            var applicationMessage = new MqttApplicationMessageBuilder()
               .WithTopic(topic)
               .WithPayload(value.ToString())
               .Build();

            await mqttClient.PublishAsync(applicationMessage, CancellationToken.None);
            
            //Also publish identity, if present
            if (iotId != String.Empty)
            {
                applicationMessage = new MqttApplicationMessageBuilder()
                       .WithTopic(iotIdTopic)
                       .WithPayload(iotId)
                       .Build();

                await mqttClient.PublishAsync(applicationMessage, CancellationToken.None);
            }
        }

        private static bool CheckIfTopicSubscribed(string incomingTopic)
        {
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
            Log.Debug("Caching value of " + incomingMessage + " for topic " +  incomingTopic);
            var cachePath = Path.Combine(histRoot, (Base64Encode(incomingTopic) + ".txt"));
            File.WriteAllText(cachePath, incomingMessage);
        }

        private static void LearnTopicsFromTopic(string incomingTopic, string incomingMessage, string seperator = "/")
        {
            if (!knownTopics.Contains(incomingTopic))
            {
                Log.Debug("Learning new simple topic: " + incomingTopic);
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
                        Log.Debug("Learning new complex topic: " + incomingTopic);
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
                        Log.Warning("Topic payload was not parseable as JSON and will be ignored: " + ex.Message);
                    }
                }
            }
        }

        private static void UpdateTopicCache()
        {
            Log.Information("Topic list now: " + JsonSerializer.Serialize(knownTopics));
            using (var sw = new StreamWriter(Path.Combine(dataRoot, topicListFile), false))
            {
                foreach (var topic in knownTopics)
                {
                    sw.WriteLine(topic);
                }
            }
        }

        private static bool CheckTopicCache(string topicToCheck)
        {
            using (var sr = new StreamReader(Path.Combine(dataRoot, topicListFile), false))
            {
                while (!sr.EndOfStream)
                {
                    if (sr.ReadLine() == topicToCheck)
                    {
                        return true;
                    }
                }
                return false;
            }
        }
        private static bool IsValidJson(string json)
        {
            if (json == null || json == String.Empty)
                return false;
            if (!json.Contains(":") && !json.Contains("{") && !json.Contains("["))
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
            var plainTextBytes = Encoding.UTF8.GetBytes(plainText);
            return Convert.ToBase64String(plainTextBytes);
        }
    }
}
