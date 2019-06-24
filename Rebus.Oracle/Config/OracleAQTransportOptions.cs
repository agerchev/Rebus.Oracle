namespace Rebus.Config
{
    public class OracleAQTransportOptions
    {
        public OracleAQTransportOptions()
        {
            EnqueueOptions = new OracleAQEnqueueOptions();
            
            DequeueOptions = new OracleAQDequeueOptions();

            MessageStorageType = AQMessageStorageType.Raw;
        }

        /// <summary>
        /// Specifies what data type to use for header and body storage. 
        /// Raw is faster, can be used with buffered messages but is limited to 2000 bytes for header and body each.
        /// Blob has a much bigger limit (4 GB - 1) * DB_BLOCK_SIZE initialization parameter (8 TB to 128 TB).
        /// </summary>
        public AQMessageStorageType MessageStorageType { get; set; }
        public OracleAQEnqueueOptions EnqueueOptions { get; set; }
        public OracleAQDequeueOptions DequeueOptions { get; set; }
        public string TableName { get; set; }
        public string InputQueueName { get; set; }
    }

    public enum AQMessageStorageType
    {
        /// <summary>
        /// Saves the message body and headers in a raw field. Maximum size: 2000 bytes
        /// </summary>
        Raw,
        /// <summary>
        /// Saves the message body and headers in a blob field. Maximum size: (4 GB - 1) * DB_BLOCK_SIZE initialization parameter (8 TB to 128 TB)
        /// </summary>
        Blob
    }

    public enum AQVisibility
    {
        OnCommit,
        Immediate
    }

    public class OracleAQEnqueueOptions
    {
        public enum AQDeliveryMode
        {
            Buffered, 
            Persistent
        }

        public OracleAQEnqueueOptions()
        {
            Visibility = AQVisibility.OnCommit;
            DeliveryMode = AQDeliveryMode.Persistent;
        }

        /// <summary>
        /// Specifies the transactional behavior of the enqueue request. Possible settings are:
        ///ON_COMMIT: The enqueue is part of the current transaction.The operation is complete when the transaction commits.This setting is the default.
        ///IMMEDIATE: The enqueue operation is not part of the current transaction, but an autonomous transaction which commits at the end of the operation.This is the only value allowed when enqueuing to a non-persistent queue.
        /// </summary>
        public AQVisibility Visibility { get; set; }

        /// <summary>
        /// The enqueuer specifies the delivery mode of the messages it wishes to enqueue in the enqueue options. It can be BUFFERED or PERSISTENT. The message properties of the enqueued message indicate the delivery mode of the enqueued message. Array enqueue is only supported for buffered messages with an array size of '1'.
        /// </summary>
        public AQDeliveryMode DeliveryMode { get; set; }

        /// <summary>
        /// Specifies a transformation that will be applied before enqueuing the message. The return type of the transformation function must match the type of the queue.
        /// </summary>
        public string Tranformation { get; set; }
    }

    public class OracleAQDequeueOptions
    {
        public enum AQDeliveryMode
        {
            Buffered,
            Persistent, 
            PersistentOrBuffered
        }

        public OracleAQDequeueOptions()
        {
            Wait = 2;
            Visibility = AQVisibility.OnCommit;
            DeliveryMode = AQDeliveryMode.Persistent;
        }

        /// <summary>
        /// Specifies the wait time if there is currently no message available which matches the search criteria. Possible settings are:
        ///-1: Wait forever.
        ///0: Do not wait.
        ///Default is 2 second.
        ///number: Wait time in seconds.
        /// </summary>
        public int Wait { get; set; }

        /// <summary>
        /// Specifies whether the new message is dequeued as part of the current transaction.The visibility parameter is ignored when using the BROWSE dequeue mode. Possible settings are:
        ///ON_COMMIT: The dequeue will be part of the current transaction.This setting is the default.
        ///IMMEDIATE: The dequeue operation is not part of the current transaction, but an autonomous transaction which commits at the end of the operation.
        /// </summary>
        public AQVisibility Visibility { get; set; }

        /// <summary>
        /// A conditional expression based on the message properties, the message data properties, and PL/SQL functions.
        ///A deq_condition is specified as a Boolean expression using syntax similar to the WHERE clause of a SQL query.This Boolean expression can include conditions on message properties, user data properties(object payloads only), and PL/SQL or SQL functions(as specified in the WHERE clause of a SQL query). Message properties include priority, corrid and other columns in the queue table
        ///To specify dequeue conditions on a message payload(object payload), use attributes of the object type in clauses.You must prefix each attribute with tab.user_data as a qualifier to indicate the specific column of the queue table that stores the payload.The deq_condition parameter cannot exceed 4000 characters.If more than one message satisfies the dequeue condition, then the order of dequeuing is undetermined.
        /// </summary>
        public string DeqCondition { get; set; }

        /// <summary>
        /// Specifies a transformation that will be applied after dequeuing the message. The source type of the transformation must match the type of the queue.
        /// </summary>
        public string Tranformation { get; set; }

        /// <summary>
        /// The dequeuer specifies the delivery mode of the messages it wishes to dequeue in the dequeue options. It can be BUFFERED or PERSISTENT or PERSISTENT_OR_BUFFERED. The message properties of the dequeued message indicate the delivery mode of the dequeued message. Array dequeue is only supported for buffered messages with an array size of '1'.
        /// </summary>
        public AQDeliveryMode DeliveryMode { get; set; }
    }
}
