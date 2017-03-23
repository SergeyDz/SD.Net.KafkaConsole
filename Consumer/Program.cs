using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using Kafka.Client.Cfg;
using Kafka.Client.Consumers;
using Kafka.Client.Helper;
using Kafka.Client.Messages;
using Kafka.Client.Producers;
using Kafka.Client.Requests;
using Kafka.Client.Responses;
using Kafka.Client.Utils;
using log4net;
using log4net.Repository.Hierarchy;


namespace KConsumer
{
    class Program
    {
        private const string ClientID = "KafkaNETLibConsoleConsumer";
        private static readonly ILog Logger = LogManager.GetLogger(typeof (Program));

        static void Main(string[] args)
        {
            int correlationID = 0;

            KafkaSimpleManagerConfiguration config = new KafkaSimpleManagerConfiguration()
            {
                FetchSize = 10,
                BufferSize = 100000,
                MaxWaitTime = 500,
                MinWaitBytes = 50,
                Zookeeper = "10.1.1.231:2181,10.1.1.232:2181,10.1.1.233:2181/kafka"
            };

            using (KafkaSimpleManager<int, Message> kafkaSimpleManager = new KafkaSimpleManager<int, Message>(config))
            {
                TopicMetadata topicMetadata = kafkaSimpleManager.RefreshMetadata(0, ClientID, correlationID++, "test", true);

                Consumer consumer = kafkaSimpleManager.GetConsumer("test", 0);

                //FetchResponse response = consumer.Fetch(Assembly.GetExecutingAssembly().ManifestModule.ToString(), "test", correlationID, 0, 0, 100, 5000, 100);
                //var messages = response.PartitionData("test", 0).GetMessageAndOffsets();

                //messages.ForEach(message => Console.WriteLine(Encoding.UTF8.GetString(message.Message.Payload)));

                //var messages = FetchAndGetMessageAndOffsetList(consumer, correlationID, "test", 0, 0, 100, 5000, 100);
                //messages.ForEach(message => Console.WriteLine(Encoding.UTF8.GetString(message.Message.Payload)));
                long offsetLast = -1;
                long l = 0;

                long totalCount = 0, offsetBase = 0, partitionID = 0, lastNotifytotalCount = 0, latest = 0, earliest = 0;

                while (true)
                {
                    correlationID++;
                    List<MessageAndOffset> messages = FetchAndGetMessageAndOffsetList(consumer, 
                                                correlationID++,
                                                "test", 
                                                0, 
                                                0,
                                                10,
                                                5000,
                                                1);



                    if (messages == null)
                    {
                        Logger.Error("PullMessage got null  List<MessageAndOffset>, please check log for detail.");
                        break;
                    }
                    else
                    {

                        #region dump response.Payload
                        if (messages.Any())
                        {
                            offsetLast = messages.Last().MessageOffset;
                            totalCount += messages.Count;
                            Logger.InfoFormat("Finish read partition {0} to {1}.  ", partitionID, offsetLast);
                            offsetBase = offsetLast + 1;
                            if (totalCount - lastNotifytotalCount > 1000)
                            {
                                Console.WriteLine("Partition: {0} totally read  {1}  will continue read from   {2}", partitionID, totalCount, offsetBase);
                                lastNotifytotalCount = totalCount;
                            }

                            // return messages
                            messages.ForEach(message => Console.WriteLine(Encoding.UTF8.GetString(message.Message.Payload)));
                        }
                        else
                        {
                            Logger.InfoFormat("Finish read partition {0} to {1}.   Earliese:{2} latest:{3} ", partitionID, offsetLast, earliest, latest);
                            Console.WriteLine("Partition: {0} totally read  {1}  Hit end of queue   {2}", partitionID, totalCount, offsetBase);
                            break;
                        }
                        #endregion
                    }
                }
            }

            Console.ReadKey();
        }

        private static List<MessageAndOffset> FetchAndGetMessageAndOffsetList(
            Consumer consumer,
            int correlationID,
            string topic,
            int partitionIndex,
            long fetchOffset,
            int fetchSize,
            int maxWaitTime,
            int minWaitSize)
        {
            List<MessageAndOffset> listMessageAndOffsets = new List<MessageAndOffset>();
            PartitionData partitionData = null;
            int payloadCount = 0;
            // at least retry once
            int maxRetry = 1;
            int retryCount = 0;
            string s = string.Empty;
            bool success = false;
            while (!success && retryCount < maxRetry)
            {
                try
                {
                    FetchResponse response = consumer.Fetch(Assembly.GetExecutingAssembly().ManifestModule.ToString(),      // client id
                        topic,
                        correlationID, //random.Next(int.MinValue, int.MaxValue),                        // correlation id
                        partitionIndex,
                        fetchOffset,
                        fetchSize,
                        maxWaitTime,
                        minWaitSize);

                    if (response == null)
                    {
                        throw new KeyNotFoundException(string.Format("FetchRequest returned null response,fetchOffset={0},leader={1},topic={2},partition={3}",
                            fetchOffset, consumer.Config.Broker, topic, partitionIndex));
                    }

                    partitionData = response.PartitionData(topic, partitionIndex);
                    if (partitionData == null)
                    {
                        throw new KeyNotFoundException(string.Format("PartitionData is null,fetchOffset={0},leader={1},topic={2},partition={3}",
                            fetchOffset, consumer.Config.Broker, topic, partitionIndex));
                    }

                    if (partitionData.Error == ErrorMapping.OffsetOutOfRangeCode)
                    {
                        s = "PullMessage OffsetOutOfRangeCode,change to Latest,topic={0},leader={1},partition={2},FetchOffset={3},retryCount={4},maxRetry={5}";
                        Logger.ErrorFormat(s, topic, consumer.Config.Broker, partitionIndex, fetchOffset, retryCount, maxRetry);
                        return null;
                    }

                    if (partitionData.Error != ErrorMapping.NoError)
                    {
                        s = "PullMessage ErrorCode={0},topic={1},leader={2},partition={3},FetchOffset={4},retryCount={5},maxRetry={6}";
                        Logger.ErrorFormat(s, partitionData.Error, topic, consumer.Config.Broker, partitionIndex, fetchOffset, retryCount, maxRetry);
                        return null;
                    }

                    success = true;
                    listMessageAndOffsets = partitionData.GetMessageAndOffsets();
                    if (null != listMessageAndOffsets && listMessageAndOffsets.Any())
                    {
                        //TODO: When key are same for sequence of message, need count payloadCount by this way.  So why line 438 work? is there bug?
                        payloadCount = listMessageAndOffsets.Count;// messages.Count();

                        long lastOffset = listMessageAndOffsets.Last().MessageOffset;

                        if ((payloadCount + fetchOffset) != (lastOffset + 1))
                        {
                            s = "PullMessage offset payloadCount out-of-sync,topic={0},leader={1},partition={2},payloadCount={3},FetchOffset={4},lastOffset={5},retryCount={6},maxRetry={7}";
                            Logger.ErrorFormat(s, topic, consumer.Config.Broker, partitionIndex, payloadCount, fetchOffset, lastOffset, retryCount, maxRetry);
                        }
                    }

                    return listMessageAndOffsets;
                }
                catch (Exception)
                {
                    if (retryCount >= maxRetry)
                    {
                        throw;
                    }
                }
                finally
                {
                    retryCount++;
                }
            } // end of while loop

            return listMessageAndOffsets;
        }
    }
}
