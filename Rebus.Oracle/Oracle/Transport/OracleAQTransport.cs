using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Serialization;
using Rebus.Time;
using Rebus.Transport;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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

        /// <summary>It can be implemented in some time.</summary>
        public void CreateQueue(string address)
        {
        }

        /// <inheritdoc />
        public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
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
        }

        async Task InnerSend(string destinationAddress, TransportMessage message, ConnectionWrapper connection)
        {
            using (var command = connection.Connection.CreateCommand())
            {
                command.CommandText = "PKG_REBUS.P_Enqueue_Rebus_Msg";

                command.CommandType = CommandType.StoredProcedure;

                var headers = message.Headers.Clone();

                var priority = GetMessagePriority(headers);
                var delay = GetDelayInSeconds(headers);
                var expiration = GetExpirationInSeconds(headers);
                var headers_keys = headers.Select(h => h.Key).ToArray();
                var headers_values = headers.Select(h => h.Value).ToArray();

                command.BindByName = true;

                command.Parameters.Add(new OracleParameter("p_Queue_Name", OracleDbType.Varchar2, destinationAddress, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("p_EO_Visibility", OracleDbType.Byte, GetVisibility(_options.EnqueueOptions.Visibility), ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("p_EO_Transformation", OracleDbType.Varchar2, _options.EnqueueOptions.Tranformation, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("p_EO_Delivery_Mode", OracleDbType.Byte, GetEnqueueDeliveryMode(_options.EnqueueOptions.DeliveryMode), ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("p_MP_Priority", OracleDbType.Int32, priority, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("p_MP_Delay", OracleDbType.Int32, delay, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("p_MP_Expiration", OracleDbType.Int32, expiration, ParameterDirection.Input));

                bool storeBodyInRaw = message.Body.Length <= 2000 && !_options.ForceBlobStore;

                var headerKeysParam = command.Parameters.Add(new OracleParameter("p_Headers_Keys", OracleDbType.Varchar2));
                headerKeysParam.Direction = System.Data.ParameterDirection.Input;
                headerKeysParam.CollectionType = OracleCollectionType.PLSQLAssociativeArray;
                headerKeysParam.Value = headers_keys;
                headerKeysParam.Size = headers_keys.Count();
                headerKeysParam.ArrayBindSize = headers_keys.Select(key => key.Length).ToArray();
                headerKeysParam.ArrayBindStatus = Enumerable.Repeat(OracleParameterStatus.Success, headers_keys.Count()).ToArray();

                var headerValuesParam = command.Parameters.Add(new OracleParameter("p_Headers_Values", OracleDbType.Varchar2));
                headerValuesParam.Direction = System.Data.ParameterDirection.Input;
                headerValuesParam.CollectionType = OracleCollectionType.PLSQLAssociativeArray;
                headerValuesParam.Value = headers_values;
                headerValuesParam.Size = headers_values.Count();
                headerValuesParam.ArrayBindSize = headers_values.Select(key => key.Length).ToArray();
                headerValuesParam.ArrayBindStatus = Enumerable.Repeat(OracleParameterStatus.Success, headers_values.Count()).ToArray();


                command.Parameters.Add(new OracleParameter("p_Body_Raw", OracleDbType.Raw, storeBodyInRaw ? message.Body : null, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("p_Body_Blob", OracleDbType.Blob, !storeBodyInRaw ? message.Body : null, ParameterDirection.Input));

                await command.ExecuteNonQueryAsync();

                headerKeysParam.Dispose();
                headerValuesParam.Dispose();
            }
        }

        /// <inheritdoc />
        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            OracleParameter headerKeysParam, headerValuesParam, bodyRawParam, bodyBlobParam;

            var connection = await GetConnection(context);

            TransportMessage receivedTransportMessage;

            using (var command = connection.Connection.CreateCommand())
            {
                command.CommandText = "PKG_REBUS.P_Dequeue_Rebus_Msg";

                command.CommandType = CommandType.StoredProcedure;

                command.Parameters.Add(new OracleParameter("p_Queue_Name", OracleDbType.Varchar2, _options.InputQueueName, ParameterDirection.Input));

                var consumersNamesParam = command.Parameters.Add(new OracleParameter("p_DO_Consumers_Names", OracleDbType.Varchar2));
                consumersNamesParam.Direction = System.Data.ParameterDirection.Input;
                consumersNamesParam.CollectionType = OracleCollectionType.PLSQLAssociativeArray;
                if (_options.DequeueOptions.ConsumersNames != null && _options.DequeueOptions.ConsumersNames.Count > 0)
                {                    
                    consumersNamesParam.Value = _options.DequeueOptions.ConsumersNames.ToArray();
                    consumersNamesParam.Size = _options.DequeueOptions.ConsumersNames.Count();
                    consumersNamesParam.ArrayBindSize = _options.DequeueOptions.ConsumersNames.Select(key => key.Length).ToArray();
                    consumersNamesParam.ArrayBindStatus = Enumerable.Repeat(OracleParameterStatus.Success, _options.DequeueOptions.ConsumersNames.Count()).ToArray();
                }
                else
                {
                    consumersNamesParam.Value = DBNull.Value;
                    consumersNamesParam.Size = 0;
                    consumersNamesParam.ArrayBindSize = new int[] { 1 };
                    consumersNamesParam.ArrayBindStatus = Enumerable.Repeat(OracleParameterStatus.Success, 1).ToArray();
                }

                command.Parameters.Add(new OracleParameter("p_DO_Visibility", OracleDbType.Byte, GetVisibility(_options.DequeueOptions.Visibility), ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("p_DO_Wait", OracleDbType.Int32, _options.DequeueOptions.Wait, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("p_DO_Transformation", OracleDbType.Varchar2, _options.DequeueOptions.Tranformation, ParameterDirection.Input));
                command.Parameters.Add(new OracleParameter("p_DO_Delivery_Mode", OracleDbType.Byte, GetDequeueDeliveryMode(_options.DequeueOptions.DeliveryMode), ParameterDirection.Input));

                command.Parameters.Add(headerKeysParam = new OracleParameter("p_Headers_Keys", OracleDbType.Varchar2, ParameterDirection.Output));
                headerKeysParam.CollectionType = OracleCollectionType.PLSQLAssociativeArray;
                headerKeysParam.Size = 100;
                headerKeysParam.ArrayBindSize = Enumerable.Repeat(4000, 100).ToArray();
                headerKeysParam.ArrayBindStatus = Enumerable.Repeat(OracleParameterStatus.Success, 100).ToArray();

                command.Parameters.Add(headerValuesParam = new OracleParameter("p_Headers_Values", OracleDbType.Varchar2, ParameterDirection.Output));
                headerValuesParam.CollectionType = OracleCollectionType.PLSQLAssociativeArray;
                headerValuesParam.Size = 100;
                headerValuesParam.ArrayBindSize = Enumerable.Repeat(4000, 100).ToArray();
                headerValuesParam.ArrayBindStatus = Enumerable.Repeat(OracleParameterStatus.Success, 100).ToArray();

                command.Parameters.Add(bodyRawParam = new OracleParameter("p_Body_Raw", OracleDbType.Raw, ParameterDirection.Output));
                bodyRawParam.Size = 2000;
                command.Parameters.Add(bodyBlobParam = new OracleParameter("p_Body_Blob", OracleDbType.Blob, ParameterDirection.Output));

                command.InitialLOBFetchSize = -1;
                command.InitialLONGFetchSize = -1;

                try
                {
                    /*We are not using the cancellationToken, bacause the driver is not implementing true async support yet. On cancel the process hangs */
                    await command.ExecuteNonQueryAsync();

                    Dictionary<string, string> header = new Dictionary<string, string>();
                    byte[] body;

                    OracleString[] headerKeys = (OracleString[])headerKeysParam.Value;
                    OracleString[] headerValues = (OracleString[])headerValuesParam.Value;

                    OracleBinary bodyBinary = ((OracleBinary)bodyRawParam.Value);
                    OracleBlob bodyBlob = (OracleBlob)bodyBlobParam.Value;

                    for (int i = 0; i < headerKeys.Length; i++)
                    {
                        if (string.IsNullOrEmpty(headerKeys[i].Value))
                        {
                            break;
                        }

                        header.Add(headerKeys[i].Value, headerValues[i].Value);
                    }

                    body = bodyBinary.IsNull ? bodyBlob.Value : bodyBinary.Value;

                    receivedTransportMessage = new TransportMessage(header, body);
                }
                catch (OracleException oracleException)
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

        /// <summary>
        /// Gets the address of the transport
        /// </summary>
        public string Address => _options.InputQueueName;

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

        static int GetVisibility(AQVisibility visibility)
        {
            if (visibility == AQVisibility.Immediate)
                //DBMS_AQ.IMMEDIATE
                return 1;
            else if (visibility == AQVisibility.OnCommit)
                //DBMS_AQ.ON_COMMIT
                return 2;
            else
                throw new NotImplementedException();
        }

        static int GetEnqueueDeliveryMode(OracleAQEnqueueOptions.AQDeliveryMode deliveryMode)
        {
            switch (deliveryMode)
            {
                case OracleAQEnqueueOptions.AQDeliveryMode.Buffered:
                    //DBMS_AQ.BUFFERED
                    return 2;
                case OracleAQEnqueueOptions.AQDeliveryMode.Persistent:
                    //DBMS_AQ.PERSISTENT
                    return 1;
                default:
                    throw new NotImplementedException();
            }
        }

        static int GetDequeueDeliveryMode(OracleAQDequeueOptions.AQDeliveryMode deliveryMode)
        {
            switch (deliveryMode)
            {
                case OracleAQDequeueOptions.AQDeliveryMode.Buffered:
                    //DBMS_AQ.BUFFERED
                    return 2;
                case OracleAQDequeueOptions.AQDeliveryMode.Persistent:
                    //DBMS_AQ.PERSISTENT
                    return 1;
                case OracleAQDequeueOptions.AQDeliveryMode.PersistentOrBuffered:
                    //DBMS_AQ.PERSISTENT_OR_BUFFERED
                    return 3;
                default:
                    throw new NotImplementedException();
            }
        }
    }
}
