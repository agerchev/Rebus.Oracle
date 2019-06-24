using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Serialization;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;
using Rebus.Config;

namespace Rebus.Oracle.Transport
{
    /// <summary>
    /// Implementation of <see cref="ITransport"/> that uses Oracle Advanced Quing to move messages around
    /// </summary>
    public class OracleAQTransport : ITransport, IDisposable
    {
        const string CurrentConnectionKey = "oracle-aq-transport-current-connection";

        static readonly HeaderSerializer HeaderSerializer = new HeaderSerializer();

        readonly OracleConnectionHelper _connectionHelper;
        readonly OracleAQTransportOptions _options;
        readonly AsyncBottleneck _receiveBottleneck = new AsyncBottleneck(20);
        readonly ILog _log;

        /// <summary>
        /// Header key of message priority which happens to be supported by this transport
        /// </summary>
        public const string MessagePriorityHeaderKey = "rbs2-msg-priority";

        const int OperationCancelledNumber = 3980;
        const int TimeOutOrEndOfFethNumber = 25228;
        const int DBMS_AQ_NEVER = -1;
        const int DBMS_AQ_NO_DELAY = 0;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionHelper"></param>
        /// <param name="options"></param>
        /// <param name="rebusLoggerFactory"></param>
        public OracleAQTransport(OracleConnectionHelper connectionHelper, OracleAQTransportOptions options, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            _log = rebusLoggerFactory.GetLogger<OracleAQTransport>();
            _connectionHelper = connectionHelper ?? throw new ArgumentNullException(nameof(connectionHelper));

            _options = options ?? throw new ArgumentNullException(nameof(options));

            if (_options.DequeueOptions == null) throw new ArgumentNullException(nameof(_options.DequeueOptions));
            if (_options.EnqueueOptions == null) throw new ArgumentNullException(nameof(_options.EnqueueOptions));

        }

        /// <summary>The SQL transport doesn't really have queues, so this function does nothing</summary>
        public void CreateQueue(string address)
        {
        }

        /// <inheritdoc />
        public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            //Stopwatch stopwatch = Stopwatch.StartNew();

            var connection = await GetConnection(context);
            var semaphore = connection.Semaphore;

            // serialize access to the connection
            await semaphore.WaitAsync();

            try
            {
                await InnerSend(destinationAddress, message, connection);
            }
            finally
            {
                semaphore.Release();
            }

            //_log.Warn("SendTime = {0}, {1}", stopwatch.ElapsedMilliseconds, DateTime.Now);
        }

        async Task InnerSend(string destinationAddress, TransportMessage message, ConnectionWrapper connection)
        {
            Debugger.Launch();
            using (var command = connection.Connection.CreateCommand())
            {
                command.CommandText = $@"
                    DECLARE
                        enqueue_options     dbms_aq.enqueue_options_t;
                        message_properties  dbms_aq.message_properties_t;
                        message_handle      RAW(16);
                    BEGIN

                    enqueue_options.visibility      := {GetVisibility(_options.EnqueueOptions.Visibility)};
                    enqueue_options.delivery_mode   := {GetEnqueueDeliveryMode(_options.EnqueueOptions.DeliveryMode)};
                    enqueue_options.transformation  := '{_options.EnqueueOptions.Tranformation}';

                    message_properties.priority     := :priority;
                    message_properties.delay        := :delay;
                    message_properties.expiration   := :expiration;

                    DBMS_AQ.ENQUEUE(
                        QUEUE_NAME => :queue_name,
                        ENQUEUE_OPTIONS => enqueue_options,
                        MESSAGE_PROPERTIES => message_properties,
                        PAYLOAD => new REBUS_MESSAGE_T(:header, :body),
                        MSGID => message_handle);
                    END;";

                command.CommandType = CommandType.Text;

                var headers = message.Headers.Clone();

                var priority = GetMessagePriority(headers);
                var delay = GetDelayInSeconds(headers);
                var expiration = GetExpirationInSeconds(headers);

                // must be last because the other functions on the headers might change them
                var serializedHeaders = HeaderSerializer.Serialize(headers);

                command.BindByName = true;

                command.Parameters.Add(new OracleParameter("priority", OracleDbType.Int32, priority, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("delay", OracleDbType.Int32, delay, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("expiration", OracleDbType.Int32, expiration, ParameterDirection.Input));

                command.Parameters.Add(new OracleParameter("queue_name", OracleDbType.Varchar2, destinationAddress, ParameterDirection.Input));

                if (_options.MessageStorageType == AQMessageStorageType.Raw)
                {
                    if (serializedHeaders.Length > 2000 ||
                        message.Body.Length > 2000)
                        throw new NotSupportedException("Message body or header size is greater than 2000 bytes, use blob for message storage type");

                    command.Parameters.Add(new OracleParameter("header", OracleDbType.Raw, serializedHeaders, ParameterDirection.Input));
                    command.Parameters.Add(new OracleParameter("body", OracleDbType.Raw, message.Body, ParameterDirection.Input));
                }
                else
                {
                    command.Parameters.Add(new OracleParameter("header", OracleDbType.Blob, serializedHeaders, ParameterDirection.Input));
                    command.Parameters.Add(new OracleParameter("body", OracleDbType.Blob, message.Body, ParameterDirection.Input));
                }

                await command.ExecuteNonQueryAsync();
            }
        }

        /// <inheritdoc />
        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            using (await _receiveBottleneck.Enter(cancellationToken))
            {
                OracleParameter headerParam, bodyParam;
                var connection = await GetConnection(context);

                TransportMessage receivedTransportMessage;

                using (var command = connection.Connection.CreateCommand())
                {
                    command.CommandText = $@"
                    DECLARE 
                       dequeue_options      dbms_aq.DEQUEUE_OPTIONS_T;
                       message_properties   dbms_aq.message_properties_t;
                       payload              REBUS_MESSAGE_T;
                       message_handle       RAW(16);
                    BEGIN
                        
                    dequeue_options.visibility      := {GetVisibility(_options.DequeueOptions.Visibility)};
                    dequeue_options.delivery_mode   := {GetDequeueDeliveryMode(_options.DequeueOptions.DeliveryMode)};
                    dequeue_options.transformation  := '{_options.DequeueOptions.Tranformation}';
                    dequeue_options.WAIT            := :wait;
                        
                        DBMS_AQ.DEQUEUE(
                            QUEUE_NAME => :queue_name,
                            DEQUEUE_OPTIONS => dequeue_options,
                            MESSAGE_PROPERTIES => message_properties,
                            PAYLOAD => payload,
                            MSGID => message_handle);
                        
                        :header := payload.HEADER;
                        :body   := payload.BODY;
                    END; ";

                    command.CommandType = CommandType.Text;

                    command.Parameters.Add(new OracleParameter("wait", OracleDbType.Int32, _options.DequeueOptions.Wait, ParameterDirection.Input));
                    command.Parameters.Add(new OracleParameter("queue_name", OracleDbType.Varchar2, _options.InputQueueName, ParameterDirection.Input));

                    if (_options.MessageStorageType == AQMessageStorageType.Raw)
                    {
                        command.Parameters.Add(headerParam = new OracleParameter("header", OracleDbType.Raw, ParameterDirection.Output));
                        headerParam.Size = 2000;

                        command.Parameters.Add(bodyParam = new OracleParameter("body", OracleDbType.Raw, ParameterDirection.Output));
                        bodyParam.Size = 2000;
                    }
                    else
                    {
                        command.Parameters.Add(headerParam = new OracleParameter("header", OracleDbType.Blob, ParameterDirection.Output));
                        command.Parameters.Add(bodyParam = new OracleParameter("body", OracleDbType.Blob, ParameterDirection.Output));
                        
                    }
                    command.InitialLOBFetchSize = -1;
                    try
                    {
                        command.ExecuteNonQuery();

                        byte[] header, body;

                        if (_options.MessageStorageType == AQMessageStorageType.Raw)
                        {
                            header = ((OracleBinary)headerParam.Value).Value;
                            body = ((OracleBinary)bodyParam.Value).Value;
                        }
                        else
                        {
                            header = (headerParam.Value as OracleBlob).Value;
                            body = (bodyParam.Value as OracleBlob).Value;
                        }

                        receivedTransportMessage = new TransportMessage(HeaderSerializer.Deserialize(header), body);
                    }
                    catch(OracleException oracleException)
                    {
                        if (oracleException.Number == TimeOutOrEndOfFethNumber)
                            receivedTransportMessage = null;
                        else
                            throw;
                    }
                    catch (SqlException sqlException) when (sqlException.Number == OperationCancelledNumber)
                    {
                        // ADO.NET does not throw the right exception when the task gets cancelled - therefore we need to do this:
                        throw new TaskCanceledException("Receive operation was cancelled", sqlException);
                    }
                }
                
                return receivedTransportMessage;
            }
        }

        /// <summary>
        /// Gets the address of the transport
        /// </summary>
        public string Address => _options.InputQueueName;

        /// <summary>
        /// Creates the necessary table
        /// </summary>
        public void EnsureQueueIsCreated()
        {
            try
            {
                CreateSchema();
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Error attempting to initialize SQL transport schema with mesages table [dbo].[{_options.TableName}]");
            }
        }

        void CreateSchema()
        {
            using (var connection = _connectionHelper.GetConnection())
            {
                var tableNames = connection.GetTableNames();

                if (tableNames.Contains(_options.TableName, StringComparer.OrdinalIgnoreCase))
                {
                    _log.Info("Database already contains a table named {tableName} - will not create anything", _options.TableName);
                    return;
                }

                _log.Info("Table {tableName} does not exist - it will be created now", _options.TableName);

                ExecuteCommands(connection, $@"

                    CREATE OR REPLACE TYPE REBUS_MESSAGE_T AS OBJECT (
                    HEADER           {(_options.MessageStorageType == AQMessageStorageType.Raw ? "RAW(2000)" : "BLOB")},
                    BODY             {(_options.MessageStorageType == AQMessageStorageType.Raw ? "RAW(2000)" : "BLOB")}
                    );

                    ----
                    begin
                    -- Call the procedure
                        sys.dbms_aqadm.create_queue_table(queue_table => '{_options.TableName}',
                                                        queue_payload_type => 'REBUS_MESSAGE_T', 
                                                        primary_instance => 1);
                                    
                        sys.dbms_aqadm.create_queue(queue_name => '{_options.InputQueueName}',
                                                    queue_table => '{_options.TableName}',
                                                    max_retries => 10, 
                                                    retry_delay => 30);

                        sys.dbms_aqadm.start_queue(queue_name => '{_options.InputQueueName}');
                    end;
                ");

                connection.Complete();
            }
        }

        static void ExecuteCommands(OracleDbConnection connection, string sqlCommands)
        {
            foreach (var sqlCommand in sqlCommands.Split(new[] { "----" }, StringSplitOptions.RemoveEmptyEntries))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sqlCommand;

                    Execute(command);
                }
            }
        }

        static void Execute(IDbCommand command)
        {
            try
            {
                command.ExecuteNonQuery();
            }
            catch (OracleException exception)
            {
                throw new RebusApplicationException(exception, $@"Error executing SQL command
{command.CommandText}
");
            }
        }

        class ConnectionWrapper : IDisposable
        {
            public ConnectionWrapper(OracleDbConnection connection)
            {
                Connection = connection;
                Semaphore = new SemaphoreSlim(1, 1);
            }

            public OracleDbConnection Connection { get; }
            public SemaphoreSlim Semaphore { get; }

            public void Dispose()
            {
                Connection?.Dispose();
                Semaphore?.Dispose();
            }
        }

        Task<ConnectionWrapper> GetConnection(ITransactionContext context)
        {
            return context
                .GetOrAdd(CurrentConnectionKey,
                    async () =>
                    {
                        await Task.CompletedTask;
                        var dbConnection = _connectionHelper.GetConnection();
                        var connectionWrapper = new ConnectionWrapper(dbConnection);
                        context.OnCommitted(() =>
                        {
                            dbConnection.Complete();
                            return Task.FromResult(0);
                        });
                        context.OnDisposed(() => connectionWrapper.Dispose());
                        return connectionWrapper;
                    });
        }


        /// <inheritdoc />
        public void Dispose()
        {
        }

        static int GetMessagePriority(Dictionary<string, string> headers)
        {
            var valueOrNull = headers.GetValueOrNull(MessagePriorityHeaderKey);
            if (valueOrNull == null) return 0;

            try
            {
                return int.Parse(valueOrNull);
            }
            catch (Exception exception)
            {
                throw new FormatException($"Could not parse '{valueOrNull}' into an Int32!", exception);
            }
        }

        static int GetDelayInSeconds(IDictionary<string, string> headers)
        {
            if (!headers.TryGetValue(Headers.DeferredUntil, out var deferredUntilDateTimeOffsetString))
            {
                return DBMS_AQ_NO_DELAY;
            }

            var deferredUntilTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

            headers.Remove(Headers.DeferredUntil);

            return (int)(deferredUntilTime - RebusTime.Now).TotalSeconds;
        }

        static int GetExpirationInSeconds(IReadOnlyDictionary<string, string> headers)
        {
            if (!headers.ContainsKey(Headers.TimeToBeReceived))
                return DBMS_AQ_NEVER;

            var timeToBeReceivedStr = headers[Headers.TimeToBeReceived];
            var timeToBeReceived = TimeSpan.Parse(timeToBeReceivedStr);

            return (int)timeToBeReceived.TotalSeconds;
        }
        
        static string GetVisibility(AQVisibility visibility)
        {
            if (visibility == AQVisibility.Immediate)
                return "DBMS_AQ.IMMEDIATE";
            else if (visibility == AQVisibility.OnCommit)
                return "DBMS_AQ.ON_COMMIT";
            else
                throw new NotImplementedException();
        }

        static string GetEnqueueDeliveryMode(OracleAQEnqueueOptions.AQDeliveryMode deliveryMode)
        {
            switch (deliveryMode)
            {
                case OracleAQEnqueueOptions.AQDeliveryMode.Buffered:
                    return "DBMS_AQ.BUFFERED";
                case OracleAQEnqueueOptions.AQDeliveryMode.Persistent:
                    return "DBMS_AQ.PERSISTENT";
                default:
                    throw new NotImplementedException();
            }
        }

        static string GetDequeueDeliveryMode(OracleAQDequeueOptions.AQDeliveryMode deliveryMode)
        {
            switch (deliveryMode)
            {
                case OracleAQDequeueOptions.AQDeliveryMode.Buffered:
                    return "DBMS_AQ.BUFFERED";
                case OracleAQDequeueOptions.AQDeliveryMode.Persistent:
                    return "DBMS_AQ.PERSISTENT";
                case OracleAQDequeueOptions.AQDeliveryMode.PersistentOrBuffered:
                    return "DBMS_AQ.PERSISTENT_OR_BUFFERED";
                default:
                    throw new NotImplementedException();
            }
        }
    }
}
