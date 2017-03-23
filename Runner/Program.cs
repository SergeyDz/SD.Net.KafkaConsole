using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Cfg;
using Kafka.Client.Consumers;
using Kafka.Client.Helper;
using Kafka.Client.Messages;
using Kafka.Client.Producers;
using Kafka.Client.Requests;

namespace Runner
{
    class Program
    {
        private const string ClientID = "KafkaNETLibConsoleConsumer";

        static void Main(string[] args)
        {
            int correlationID = 0;
            string topic = "t4";

            KafkaSimpleManagerConfiguration config = new KafkaSimpleManagerConfiguration()
            {
                FetchSize = 100,
                BufferSize = 100,
                MaxWaitTime = 5000,
                MinWaitBytes = 1,
                Zookeeper = "10.1.1.231:2181,10.1.1.232:2181,10.1.1.233:2181/kafka"
            };

            ProducerConfiguration producerConfiguration = new ProducerConfiguration(new []{new BrokerConfiguration()});
            config.Verify();

            using (KafkaSimpleManager<int, Message> kafkaSimpleManager = new KafkaSimpleManager<int, Message>(config))
            {
                TopicMetadata topicMetadata = kafkaSimpleManager.RefreshMetadata(0, ClientID, correlationID++, topic, true);

                kafkaSimpleManager.InitializeProducerPoolForTopic(0, ClientID, correlationID, topic, true,
                    producerConfiguration, true);

                var producer1 = kafkaSimpleManager.GetProducerOfPartition(topic, 0, true);
                var producer2 = kafkaSimpleManager.GetProducerOfPartition(topic, 4, true);

                for (int i = 0; i < 100; i++)
                {
                    var producer = i%2 == 0 ? producer1 : producer2;
                    var tKey = Encoding.UTF8.GetBytes(DateTime.Now.Ticks.ToString());
                    var tValue = Encoding.UTF8.GetBytes("Hello world " + i);
                    producer.Send(new ProducerData<int, Message>(topic,
                        new Message(tValue, tKey, CompressionCodecs.DefaultCompressionCodec)));
                }

                producer1.Dispose();
                producer2.Dispose();

                Console.WriteLine("Topic is: " + topicMetadata.Topic);
            }

            //Console.ReadKey();
        }
    }
}
