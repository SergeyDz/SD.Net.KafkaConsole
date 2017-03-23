using System;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            var options = new KafkaOptions(new Uri("http://sdzyuban-mesos-01:31000"));
            var router = new BrokerRouter(options);
            var client = new KafkaNet.Producer(router);

            client.SendMessageAsync("test", new[] { new Message("hello world") }).Wait();

            using (client) { }
        }
    }
}
