using Rebus.Workers.ThreadPoolBased;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.Retry
{
    public class NoopBackofStrategy : IBackoffStrategy
    {
        public void Reset()
        {
        }

        public void Wait(CancellationToken token)
        {
        }

        public Task WaitAsync()
        {
            return Task.CompletedTask;
        }

        public void WaitError()
        {

        }

        public Task WaitErrorAsync(CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public void WaitNoMessage()
        {

        }

        public Task WaitNoMessageAsync(CancellationToken token)
        {
            return Task.CompletedTask;
        }
    }
}
