using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace MindSung.Messaging
{
    public class MessageClient : IDisposable
    {
        public MessageClient(IPAddress hostAddress, int port, int numConnections = 1, int recycleTimeMs = 0)
        {
            this.address = hostAddress;
            this.port = port;
            this.recycleTimeMs = recycleTimeMs;
            tcpConnections = new TcpConnection[numConnections];
            for (int i = 0; i < numConnections; i++)
            {
                tcpConnections[i] = new TcpConnection(this, recycleTimeMs);
            }
        }

        public MessageClient(IPEndPoint endPoint, int numConnections = 1, int recycleTimeMs = 0)
            : this(endPoint.Address, endPoint.Port, numConnections, recycleTimeMs)
        {
        }

        public MessageClient(int port, int numConnections = 1, int recycleTimeMs = 0)
            : this(IPAddress.Loopback, port, numConnections, recycleTimeMs)
        {
        }

        public int CommandTimeout { get; set; }
        public bool KeepAlive { get; set; }

        protected virtual Task OnMessage(MessageConnection connection, Message message)
        {
            return Task.FromResult(true);
        }

        IPAddress address;
        int port;
        TcpConnection[] tcpConnections;
        int recycleTimeMs;
        int icn = 0;

        public async Task<Message> SendMessage(int cmd, byte[] data = null, byte[] args = null)
        {
            var iFirst = -1;
            return await Retry.Do(async () =>
            {
                TcpConnection tcpCn = null;
                lock (tcpConnections)
                {
                    if (iFirst < 0)
                    {
                        // First attempt.
                        var i = iFirst = icn++;
                        if (icn >= tcpConnections.Length) icn = 0;
                        tcpCn = tcpConnections[i];
                        if (tcpCn.IsExpired)
                        {
                            tcpCn.CloseConnection();
                            tcpConnections[i] = tcpCn = new TcpConnection(this, recycleTimeMs);
                        }
                    }
                    else
                    {
                        // Retry, always create a new connection on retry.
                        tcpConnections[iFirst].CloseConnection();
                        tcpConnections[iFirst] = tcpCn = new TcpConnection(this, recycleTimeMs);
                    }
                    tcpCn.AddRef();
                }
                try
                {
                    var msgCn = await tcpCn.GetConnection();

                    if (CommandTimeout <= 0) return await msgCn.SendMessage(cmd, data, args);

                    var cts = new CancellationTokenSource();
                    var timeout = Task.Delay(CommandTimeout, cts.Token);
                    var message = msgCn.SendMessage(cmd, data, args);
                    if (await Task.WhenAny(message, timeout) == message)
                    {
                        cts.Cancel();
                        return message.Result;
                    }
                    throw new TimeoutException();
                }
                finally
                {
                    if (tcpCn != null) tcpCn.RemoveRef();
                }
            });
        }

        public void Dispose()
        {
            lock (tcpConnections)
            {
                foreach (var cn in tcpConnections) cn.CloseConnection();
            }
        }

        class TcpConnection
        {
            public TcpConnection(MessageClient client, int expireMs)
            {
                this.client = client;
                this.expireMs = expireMs;
                // Initially set max expire time, if expiry is requested, it will be
                // set at the time the connection is made.
                expireTime = DateTime.MaxValue;
            }

            TcpClient tcp;
            MessageClient client;
            int expireMs;
            DateTime expireTime;
            Task<MessageConnection> connectionTask;
            MessageConnection connection;
            bool connecting;
            bool closeConnection = false;
            bool closed = false;
            int refCount = 0;
            object sync = new object();

            public bool IsConnected => connection != null && !connection.Aborted && tcp != null && tcp.Connected; 
            public bool IsExpired => DateTime.Now > expireTime;

            public Task<MessageConnection> GetConnection()
            {
                if (!IsConnected)
                {
                    lock (sync)
                    {
                        if (!connecting && !IsConnected)
                        {
                            connecting = true;
                            tcp = new TcpClient();
                            connectionTask = Task.Run(async () =>
                            {
                                try
                                {
                                    await tcp.ConnectAsync(client.address, client.port);
                                    if (client.KeepAlive)
                                    {
                                        tcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                                    }
                                    tcp.NoDelay = true;
                                    tcp.SendTimeout = client.CommandTimeout;
                                    connection = new MessageConnection(tcp, (cn, msg) => client.OnMessage(cn, msg));
                                    connection.Start();
                                    if (expireMs > 0)
                                    {
                                        expireTime = DateTime.Now.AddMilliseconds(expireMs);
                                    }
                                    return connection;
                                }
                                finally
                                {
                                    connecting = false;
                                }
                            });
                        }
                    }
                }
                return connectionTask;
            }

            public void AddRef()
            {
                lock (sync)
                {
                    refCount++;
                }
            }

            public void RemoveRef()
            {
                lock (sync)
                {
                    refCount--;
                    CheckCloseConnection();
                }
            }

            private void CheckCloseConnection()
            {
                if (refCount == 0 && closeConnection && !closed)
                {
                    closed = true;
                    var nowait = Task.Run(async () =>
                    {
                        var cts = new CancellationTokenSource();
                        var timeout = Task.Delay(5000, cts.Token);
                        var message = (await GetConnection()).EndConnection();
                        if (await Task.WhenAny(message, timeout) == message)
                        {
                            cts.Cancel();
                        }
#if NET451
                        tcp.Close();
#else
                        tcp.Dispose();
#endif
                    });
                }
            }

            public void CloseConnection()
            {
                lock (sync)
                {
                    closeConnection = true;
                    CheckCloseConnection();
                }
            }
        }
    }
}
