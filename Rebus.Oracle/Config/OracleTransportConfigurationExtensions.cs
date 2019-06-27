﻿using System;
using Rebus.Logging;
using Rebus.Oracle;
using Rebus.Oracle.Transport;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.Threading;
using Rebus.Timeouts;
using Rebus.Transport;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for the SQL transport
    /// </summary>
    public static class OracleTransportConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use Oracle as its transport. The table specified by <paramref name="tableName"/> will be used to
        /// store messages, and the "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        public static void UseOracle(this StandardConfigurer<ITransport> configurer, string connectionStringOrConnectionOrConnectionStringName, string tableName, string inputQueueName, bool enlistInAmbientTransaction = false)
        {
            Configure(configurer, loggerFactory => new OracleConnectionHelper(connectionStringOrConnectionOrConnectionStringName, enlistInAmbientTransaction), tableName, inputQueueName);
        }

        /// <summary>
        /// Configures Rebus to use Oracle to transport messages as a one-way client (i.e. will not be able to receive any messages).
        /// The table specified by <paramref name="tableName"/> will be used to store messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        public static void UseOracleAsOneWayClient(this StandardConfigurer<ITransport> configurer, string connectionStringOrConnectionStringName, string tableName, bool enlistInAmbientTransaction = false)
        {
            Configure(configurer, loggerFactory => new OracleConnectionHelper(connectionStringOrConnectionStringName, enlistInAmbientTransaction), tableName, null);

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);
        }

        static void Configure(StandardConfigurer<ITransport> configurer, Func<IRebusLoggerFactory, OracleConnectionHelper> connectionProviderFactory, string tableName, string inputQueueName)
        {
            configurer.Register(context =>
            {
                var rebusLoggerFactory = context.Get<IRebusLoggerFactory>();
                var asyncTaskFactory = context.Get<IAsyncTaskFactory>();
                var connectionProvider = connectionProviderFactory(rebusLoggerFactory);
                var transport = new OracleTransport(connectionProvider, tableName, inputQueueName, rebusLoggerFactory, asyncTaskFactory);
                transport.EnsureTableIsCreated();
                return transport;
            });

            configurer.OtherService<ITimeoutManager>().Register(c => new DisabledTimeoutManager());

            configurer.OtherService<IPipeline>().Decorate(c =>
            {
                var pipeline = c.Get<IPipeline>();

                return new PipelineStepRemover(pipeline)
                    .RemoveIncomingStep(s => s.GetType() == typeof(HandleDeferredMessagesStep));
            });
        }

        /// <summary>
        /// Configures Rebus to use Oracle as its transport. The table specified by <paramref name="tableName"/> will be used to
        /// store messages, and the "queue" specified by <paramref name="inputQueueName"/> will be used when querying for messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        public static void UseOracleAQ(this StandardConfigurer<ITransport> configurer, string connectionStringOrConnectionOrConnectionStringName, OracleAQTransportOptions options, bool enlistInAmbientTransaction = false)
        {
            ConfigureAQ(configurer, loggerFactory => new OracleConnectionHelper(connectionStringOrConnectionOrConnectionStringName, enlistInAmbientTransaction), options);
        }

        /// <summary>
        /// Configures Rebus to use Oracle to transport messages as a one-way client (i.e. will not be able to receive any messages).
        /// The table specified by <paramref name="tableName"/> will be used to store messages.
        /// The message table will automatically be created if it does not exist.
        /// </summary>
        public static void UseOracleAQAsOneWayClient(this StandardConfigurer<ITransport> configurer, string connectionStringOrConnectionStringName, OracleAQTransportOptions options, bool enlistInAmbientTransaction = false)
        {
            ConfigureAQ(configurer, loggerFactory => new OracleConnectionHelper(connectionStringOrConnectionStringName, enlistInAmbientTransaction), options);

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);
        }

        static void ConfigureAQ(StandardConfigurer<ITransport> configurer, Func<IRebusLoggerFactory, OracleConnectionHelper> connectionProviderFactory, OracleAQTransportOptions options)
        {
            configurer.Register(context =>
            {
                var rebusLoggerFactory = context.Get<IRebusLoggerFactory>();
                var connectionProvider = connectionProviderFactory(rebusLoggerFactory);
                var transport = new OracleAQTransport(connectionProvider, options, rebusLoggerFactory);
                if(options.InitAQSchema)
                {
                    OracleAQSchemaInitializer oracleAQSchemaInitializer = new OracleAQSchemaInitializer(rebusLoggerFactory, connectionProvider);
                    oracleAQSchemaInitializer.EnsureQueueIsCreated(new OracleAQInitializationOptions() { InputQueueName = options.InputQueueName, TableName = options.TableName });
                }
                return transport;
            });

            configurer.OtherService<ITimeoutManager>().Register(c => new DisabledTimeoutManager());

            configurer.OtherService<IPipeline>().Decorate(c =>
            {
                var pipeline = c.Get<IPipeline>();

                return new PipelineStepRemover(pipeline)
                    .RemoveIncomingStep(s => s.GetType() == typeof(HandleDeferredMessagesStep));
            });
        }
    }
}
