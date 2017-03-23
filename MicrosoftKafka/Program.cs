using System;
using System.IO;
using KafkaNET.Library.Examples;
using log4net;

namespace MicrosoftKafka
{
    class Program
    {
        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure(new FileInfo("log4net.config"));
            var logger = LogManager.GetLogger(typeof(Program));
            logger.Info("MicrosoftKafka.Program started");

            ConsumeGroupHelperOptions ops = new ConsumeGroupHelperOptions();
            ConsumerGroupHelper.DumpMessageAsConsumerGroup(ops);

            logger.Info("MicrosoftKafka.Program ended");

            Console.ReadKey();
        }
    }
}
