using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rebus.Bus;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.ServiceProvider
{
    /// <summary>
    /// The service controls the lifetime of the resbus instance. 
    /// </summary>
    public class RebusHostingService : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;

        public RebusHostingService(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {            
            using (_serviceProvider.GetRequiredService<IBus>())
            {
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
                }
                catch (TaskCanceledException)
                {
                }
            }
        }
    }
}
