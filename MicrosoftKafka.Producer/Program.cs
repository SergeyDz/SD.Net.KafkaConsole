using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace MicrosoftKafka.Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure(new FileInfo("log4net.config"));
            var logger = LogManager.GetLogger(typeof(Program));
            logger.Info("MicrosoftKafka.Program started");

            ProduceSimpleHelper.Run(new ProduceSimpleHelperOption());

            logger.Info("MicrosoftKafka.Program ended");

        }
    }
}
