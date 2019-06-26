using System;
using System.Threading;

namespace Rebus.Oracle.Transport
{
    internal class ConnectionWrapper : IDisposable
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
}
