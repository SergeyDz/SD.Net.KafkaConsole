using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;

namespace SimpleConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var options = new KafkaOptions(new Uri("http://sdzyuban-mesos-01:31000"));
            var router = new BrokerRouter(options);
            var consumer = new Consumer(new ConsumerOptions("test", router));

            //Consume returns a blocking IEnumerable (ie: never ending stream)
            foreach (var message in consumer.Consume())
            {
                Console.WriteLine("Response: P{0},O{1} : {2}",
                    message.Meta.PartitionId, message.Meta.Offset, message.Value);
            }
        }
    }
}
