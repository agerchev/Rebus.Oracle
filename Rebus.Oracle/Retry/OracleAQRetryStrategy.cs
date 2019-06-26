using Rebus.Pipeline;
using System;
using System.Threading.Tasks;

namespace Rebus.Retry
{
    public class OracleAQRetryStrategy : IRetryStrategy
    {
        class NoopRetryStrategy : IRetryStrategyStep
        {
            public Task Process(IncomingStepContext context, Func<Task> next)
            {
                return next();
            }
        }

        public IRetryStrategyStep GetRetryStep()
        {
            return new NoopRetryStrategy();
        }
    }
}
