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
using System.Linq;

namespace SmipMqttService
{
    internal class MqttService
    {
        static string dataRoot, logPath, iotIdPath = "";
        static string iotId;
        static string histRoot = "MqttHist";
        static int heartbeatSeconds = 5;
        static string heartbeatTopic = "Smip.Mqtt.Connector.Heartbeat";
        static string iotIdTopic = "Smip.Mqtt.Connector.IotId";
        static string topicListFile = "MqttTopicList.txt";
        static string topicSubscriptionFile = "CloudAcquiredTagList.txt";
        static string topicSeperator, virtualTopicSeperator;
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
            topicListFile = Path.Combine (dataRoot, topicListFile);
            if (!File.Exists(topicListFile))
                File.Create(topicListFile).Dispose();
            else
                knownTopics = LoadKnownTopics();
            topicSubscriptionFile = Path.Combine(dataRoot, topicSubscriptionFile);
            if (!File.Exists(topicSubscriptionFile))
                File.Create(topicSubscriptionFile).Dispose();
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

            // Setup Mqtt Connection (using config)
            var mqttConfig = configuration.GetSection("Mqtt");
            string broker = mqttConfig.GetSection("brokerHost").Value;
            int port;
            if (!int.TryParse(mqttConfig.GetSection("brokerPort").Value, out port))
            {
                port = 1883;
            }

            Log.Information("SMIP MQTT Service Started with Data Root: " + dataRoot);
            Log.Information("Using MQTT Broker at: " + broker + ":" + port);
            Log.Information("Logging to: " + logPath);

            // Seed Topic file (if necessary)
            if (iotId != String.Empty && !CheckTopicCache(iotIdTopic))
                knownTopics.Add(iotIdTopic);
            if (!CheckTopicCache(heartbeatTopic))
                knownTopics.Add(heartbeatTopic);
            UpdateTopicCache();

            //Setup topic parsing
            topicSeperator = mqttConfig.GetSection("topicSeperator").Value;
            if (topicSeperator == null)
                topicSeperator = String.Empty;
            virtualTopicSeperator = mqttConfig.GetSection("virtualTopicSeperator").Value;
            if (virtualTopicSeperator == null)
                virtualTopicSeperator = String.Empty;

            //Creat MQTT connection
            bool tlsValue = false;
            string tlsSetting = mqttConfig.GetSection("brokerTLS").Value;
            if (tlsSetting != null)
            {
                Boolean.TryParse(tlsSetting, out tlsValue);
            }
            string clientId = "smipgw-" + Guid.NewGuid().ToString();
            var options = BuildMqttClientOptions(broker, port, tlsValue, clientId, mqttConfig.GetSection("brokerUser").Value, mqttConfig.GetSection("brokerPass").Value);
            var connectResult = await mqttClient.ConnectAsync(options);
            if (connectResult.ResultCode == MqttClientConnectResultCode.Success)
            {
                Log.Information("Connected to MQTT broker: " + broker);
                await mqttClient.SubscribeAsync("#");
                mqttClient.ApplicationMessageReceivedAsync += e => {
                    if (e.ApplicationMessage.Topic == heartbeatTopic || e.ApplicationMessage.Topic == iotIdTopic)   //Don't show own messages unless in debug
                        Log.Debug("Incoming message for topic: " + e.ApplicationMessage.Topic);
                    else
                        Log.Information("Incoming message for topic: " + e.ApplicationMessage.Topic);
                    if (e.ApplicationMessage.Payload != null)
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

        static MqttClientOptions BuildMqttClientOptions(string Server, int Port, bool UseTls, string ClientID, string Username, string Password)
        {
            MqttClientOptionsBuilder builder = new MqttClientOptionsBuilder()
                .WithTcpServer(Server, Port);
            if (UseTls)
                builder.WithTls();
            if (!string.IsNullOrEmpty(ClientID))
                builder.WithClientId(ClientID);
            if (!string.IsNullOrEmpty(Username))
                builder.WithCredentials(Username, Password);
            builder.WithCleanSession();
            return builder.Build();
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
                    if (unCompoundTopic.IndexOf(virtualTopicSeperator) > -1)
                    {
                        unCompoundTopic = unCompoundTopic.Substring(0, unCompoundTopic.IndexOf(virtualTopicSeperator));
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

        private static void LearnTopicsFromTopic(string incomingTopic, string incomingMessage)
        {
            if (!incomingTopic.Contains(virtualTopicSeperator) || virtualTopicSeperator == String.Empty) //Learn parts of topic name (unless virtual topic)
            {
                if (topicSeperator != String.Empty)
                {
                    var topicParts = incomingTopic.Split(topicSeperator);
                    var previousPart = String.Empty;
                    foreach (var topicPart in topicParts)
                    {
                        if ((topicSeperator + topicPart + topicSeperator) != virtualTopicSeperator)
                        {
                            if (previousPart != String.Empty)
                                previousPart = previousPart + topicSeperator + topicPart;
                            else
                                previousPart = topicPart;
                            if (!knownTopics.Contains(previousPart))
                            {
                                Log.Information("Learning new topic part: " + previousPart);
                                knownTopics.Add(previousPart);
                            }
                        }
                    }
                }
                else
                {
                    if (!knownTopics.Contains(incomingTopic))
                    {
                        Log.Information("Learning new topic: " + incomingTopic);
                        knownTopics.Add(incomingTopic);
                    }
                }
                
            }
            else //Learn virtual topic
            {
                if (!knownTopics.Contains(incomingTopic) && virtualTopicSeperator != String.Empty)
                {
                    Log.Information("Learning new virtual topic: " + incomingTopic);
                    knownTopics.Add(incomingTopic);
                }
            }
            //Discover virtual topics from body
            if (IsValidJson(incomingMessage) && virtualTopicSeperator != String.Empty)
            {
                var options = new JsonDocumentOptions
                {
                    AllowTrailingCommas = true,
                    CommentHandling = JsonCommentHandling.Skip
                };
                using (JsonDocument document = JsonDocument.Parse(incomingMessage, options))
                {
                    Log.Information("Examing payload for new virtual topics: " + incomingTopic);
                    LearnTopicsFromPayload(incomingTopic, incomingMessage, virtualTopicSeperator);
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

        #region Topic Cache Management
        //TODO: This group of methods are ridiciulously ineffecient.
        //  The only reasons to read and write from files is to make them easier to examine for debugging,
        //  and provide some persistence between runs.
        private static void UpdateTopicCache()
        {
            var previousTopics = LoadKnownTopics();
            if (!knownTopics.SequenceEqual(previousTopics))
            {
                Log.Information("Topic list now: " + JsonSerializer.Serialize(knownTopics));
                using (var sw = new StreamWriter(topicListFile, false))
                {
                    foreach (var topic in knownTopics)
                    {
                        sw.WriteLine(topic);
                    }
                }
            }
        }
        private static bool CheckTopicCache(string topicToCheck)
        {
            using (var sr = new StreamReader(topicListFile, false))
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

        private static List<string> LoadKnownTopics()
        {
            List<string> previousTopics = new List<string>();
            using (var sr = new StreamReader(topicListFile, false))
            {
                while (!sr.EndOfStream)
                {
                    previousTopics.Add(sr.ReadLine());
                }
            }
            return previousTopics;
        }
        #endregion
        
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
