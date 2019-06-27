using Oracle.ManagedDataAccess.Client;
using Rebus.Config;
using Rebus.Exceptions;
using Rebus.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Rebus.Oracle.Transport
{
    public class OracleAQInitializationOptions
    {
        public OracleAQInitializationOptions()
        {
        }

        public string TableName { get; set; }
        public string InputQueueName { get; set; }
    }

    public class OracleAQSchemaInitializer
    {
        private readonly OracleConnectionHelper _connectionHelper;
        private readonly ILog _log;
        public OracleAQSchemaInitializer(IRebusLoggerFactory loggerFactory, OracleConnectionHelper connectionHelper)
        {
            _log = loggerFactory.GetLogger<OracleAQSchemaInitializer>();
            _connectionHelper = connectionHelper;
        }

        /// <summary>
        /// Creates the necessary table
        /// </summary>
        public void EnsureQueueIsCreated(OracleAQInitializationOptions options)
        {
            try
            {
                CreateSchema(options);
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Error attempting to initialize SQL transport schema with mesages table [dbo].[{options.TableName}]");
            }
        }

        void CreateSchema(OracleAQInitializationOptions options)
        {
            using (var connection = _connectionHelper.GetConnection())
            {
                var tableNames = connection.GetTableNames();

                if (tableNames.Contains(options.TableName, StringComparer.OrdinalIgnoreCase))
                {
                    _log.Info("Database already contains a table named {tableName} - will not create anything", options.TableName);
                    return;
                }

                _log.Info("Table {tableName} does not exist - it will be created now", options.TableName);

                ExecuteCommands(connection, $@"

                    CREATE OR REPLACE TYPE REBUS_MESSAGE AS OBJECT (
                        HEADER_RAW      RAW(2000),
                        HEADER_BLOB     BLOB,
                        BODY_RAW        RAW(2000),
                        BODY_BLOB       BLOB
                    );

                    ----
                    begin
                    -- Call the procedure
                        sys.dbms_aqadm.create_queue_table(queue_table => '{options.TableName}',
                                                        queue_payload_type => 'REBUS_MESSAGE', 
                                                        primary_instance => 1);
                                    
                        sys.dbms_aqadm.create_queue(queue_name => '{options.InputQueueName}',
                                                    queue_table => '{options.TableName}',
                                                    max_retries => 10, 
                                                    retry_delay => 30);

                        sys.dbms_aqadm.start_queue(queue_name => '{options.InputQueueName}');
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
    }
}
