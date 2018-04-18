using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace MindSung.Messaging
{
    public abstract class MessageServer : IDisposable
    {
        public MessageServer(IPEndPoint endPoint)
        {
            listener = new TcpListener(endPoint);
        }

        public MessageServer(int port)
            : this(new IPEndPoint(IPAddress.Any, port))
        {
        }

        public int CommandTimeout { get; set; }

        protected abstract Task OnMessage(MessageConnection connection, Message message);

        TcpListener listener;
        List<TcpConnectionInfo> tcpConnections = new List<TcpConnectionInfo>();
        TaskCompletionSource<bool> tcsDone;

        void Accept()
        {
            var nowait = listener.AcceptTcpClientAsync().ContinueWith(t =>
            {
                if (!t.IsFaulted)
                {
                    Accept();
                    var tcp = t.Result;
                    tcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                    tcp.NoDelay = true;
                    tcp.SendTimeout = CommandTimeout;
                    var connection = new MessageConnection(tcp, (cn, msg) => { try { OnMessage(cn, msg); } catch { } });
                    lock (tcpConnections) tcpConnections.Add(new TcpConnectionInfo { server = tcp, connection = connection });
                    connection.Start();
                }
                else
                {
                    tcsDone.SetResult(true);
                }
            });
        }

        public virtual Task Start()
        {
            lock (listener)
            {
                if (tcsDone != null && !tcsDone.Task.IsCompleted) throw new InvalidOperationException("The TCP listener has already started.");

                tcsDone = new TaskCompletionSource<bool>();
                listener.Start();
                Accept();
            }
            return Task.FromResult(true);
        }

        public virtual Task<bool> Stop()
        {
            lock (listener)
            {
                if (tcsDone == null || tcsDone.Task.IsCompleted) return Task.FromResult(false);

                listener.Stop();

                lock (tcpConnections)
                {
                    foreach (var cn in tcpConnections)
                    {
                        var nowait = cn.connection.Stop();
#if NET451
                        cn.server.Close();
#else
                        cn.server.Dispose();
#endif
                    }
                    tcpConnections.Clear();
                }

                return tcsDone.Task;
            }
        }

        public void Dispose()
        {
            var nowait = Stop();
        }

        class TcpConnectionInfo
        {
            public TcpClient server;
            public MessageConnection connection;
        }
    }
}
