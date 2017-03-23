namespace MicrosoftKafka.Producer
{
    public class ProduceSimpleHelperOption
    {
        public string Zookeeper = "10.1.1.231:2181,10.1.1.232:2181,10.1.1.233:2181/kafka";
        public string PartitionerClass;
        public short RequiredAcks = 0;
        public int AckTimeout = 5000;
        public int SendTimeout  = 5000;
        public int ReceiveTimeout = 5000;
        public int BufferSize = 100;
        public int SyncProducerOfOneBroker = 1;
        public int MessageSize = 1;
        public string Topic = "t4";
        public int PartitionId = -1;
        public long MessageCountPerBatch = 10;
        public int BatchCount;
        public bool ConstantMessageKey;
        public Kafka.Client.Messages.CompressionCodecs CompressionCodec;
    }
}
