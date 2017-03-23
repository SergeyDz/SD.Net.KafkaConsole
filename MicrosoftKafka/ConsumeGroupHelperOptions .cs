namespace MicrosoftKafka
{
    public class ConsumeGroupHelperOptions
    {
        public string ConsumerGroupName = "SDConsumerGroup";
        public string ConsumerId = "SD";
        public int Timeout = 1000;
        public string Zookeeper = "10.1.1.231:2181,10.1.1.232:2181,10.1.1.233:2181/kafka";
        public int BufferSize = 2000;
        public int FetchSize = 2000;
        public int MaxFetchBufferLength = 20000;
        public int CancellationTimeoutMs = 5000;
        public string Topic = "t4";
        public int FetchThreadCountPerConsumer = 1;
        public int CommitBatchSize = 10;
        public bool CommitOffsetWithPartitionIDOffset = true;
        public int SleepTypeWhileAlwaysRead = 0;
        public bool UseSharedStaticZookeeperClient = true;


        public int ZookeeperConnectorCount = 1;
        public int Count;
        public int[] ZookeeperConnectorConsumeMessageCount = new []{1};
    }
}
