﻿using NUnit.Framework;
using Rebus.Logging;
using Rebus.Oracle.Timeouts;
using Rebus.Tests.Contracts.Timeouts;
using Rebus.Timeouts;

namespace Rebus.Oracle.Tests.Timeouts
{
    [TestFixture, Category(TestCategory.Oracle)]
    public class TestOracleTimeoutManager : BasicStoreAndRetrieveOperations<OracleTimeoutManagerFactory>
    {
    }

    public class OracleTimeoutManagerFactory : ITimeoutManagerFactory
    {
        public OracleTimeoutManagerFactory()
        {
            OracleTestHelper.DropTableAndSequence("timeouts");
        }

        public ITimeoutManager Create()
        {
            var OracleTimeoutManager = new OracleTimeoutManager(OracleTestHelper.ConnectionHelper, "timeouts", new ConsoleLoggerFactory(false));
            OracleTimeoutManager.EnsureTableIsCreated();
            return OracleTimeoutManager;
        }

        public void Cleanup()
        {
            OracleTestHelper.DropTableAndSequence("timeouts");
        }

        public string GetDebugInfo()
        {
            return "could not provide debug info for this particular timeout manager.... implement if needed :)";
        }
    }

}