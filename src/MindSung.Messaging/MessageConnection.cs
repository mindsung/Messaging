using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace MindSung.Messaging
{
    public class MessageConnection
    {
        public MessageConnection(TcpClient tcp, Func<MessageConnection, Message, Task> messageHandler)
        {
            this.tcp = tcp;
            this.messageHandler = messageHandler;
        }

        public MessageConnection(TcpClient tcp, Action<MessageConnection, Message> messageHandler)
            : this(tcp, (connection, msg) => { messageHandler(connection, msg); return Task.FromResult(true); })
        {
        }

        public void Start()
        {
            lock (this)
            {
                if (started) throw new Exception("The connection was already started.");
                tcsStop = new TaskCompletionSource<bool>();
                recvTask = ReceiveLoop();
                sendTask = SendLoop();
                started = true;
            }
        }

        public async Task Stop()
        {
            lock (this)
            {
                if (!started) Task.FromResult(true);
                tcsStop.TrySetResult(true);
                started = false;
            }
            if (sendTask != null && recvTask != null)
            {
                await Task.WhenAll(sendTask, recvTask);
            }
            if (tcp != null)
            {
                try
                {
#if NET451
                    tcp.Close();
#else
                    tcp.Dispose();
#endif
                }
                catch { }
            }
        }

        const ulong HeaderSignature = 0xEADFEADFEADFEADF;
        const int HeaderSize = 21;
        const int KeepAliveCommand = int.MinValue;
        const int KeepAliveResponse = int.MinValue + 1;
        const int EndConnectionCommand = int.MinValue + 2;

        bool started = false;
        TcpClient tcp;
        Func<MessageConnection, Message, Task> messageHandler;
        ConcurrentQueue<Message> sendQueue = new ConcurrentQueue<Message>();
        SemaphoreSlim sendReady = new SemaphoreSlim(0);
        ConcurrentDictionary<int, TaskCompletionSource<Message>> recvMap = new ConcurrentDictionary<int, TaskCompletionSource<Message>>();
        TaskCompletionSource<bool> tcsStop;
        Task sendTask;
        Task recvTask;
        static int nextId = 0;

        public bool Aborted { get; private set; }

        public static byte[] MsgHeaderToBytes(int id, int cmd, int argsLength, int dataLength)
        {
            if (argsLength > 255) throw new Exception("Message arguments length can be no more than 255 bytes.");
            var bytes = new byte[HeaderSize];
            var bi = 0;
            bytes[bi++] = (byte)argsLength;
            for (int i = 0; i < 64; i += 8, bi++) bytes[bi] = (byte)(HeaderSignature >> i & 0xFF);
            for (int i = 0; i < 32; i += 8, bi++) bytes[bi] = (byte)(id >> i & 0xFF);
            for (int i = 0; i < 32; i += 8, bi++) bytes[bi] = (byte)(cmd >> i & 0xFF);
            for (int i = 0; i < 32; i += 8, bi++) bytes[bi] = (byte)(dataLength >> i & 0xFF);
            return bytes;
        }

        public static void MsgHeaderFromBytes(byte[] bytes, out int id, out int cmd, out int argsLength, out int dataLength)
        {
            if (bytes.Length < HeaderSize) throw new Exception("Message header incorrect size.");
            ulong hs = 0;
            id = 0;
            argsLength = 0;
            dataLength = 0;
            cmd = 0;
            var bi = 0;
            argsLength = bytes[bi++];
            for (int i = 0; i < 64; i += 8, bi++) hs |= (ulong)bytes[bi] << i;
            if (hs != HeaderSignature) throw new Exception("Invalid message signature.");
            for (int i = 0; i < 32; i += 8, bi++) id |= (int)bytes[bi] << i;
            for (int i = 0; i < 32; i += 8, bi++) cmd |= (int)bytes[bi] << i;
            for (int i = 0; i < 32; i += 8, bi++) dataLength |= (int)bytes[bi] << i;
        }

        async Task SendLoop()
        {
            try
            {
                var ready = sendReady.WaitAsync();
                var stop = tcsStop.Task;
                Message msg;
                while (await Task.WhenAny(ready, stop) != tcsStop.Task)
                {
                    if (sendQueue.TryDequeue(out msg))
                    {
                        var header = MsgHeaderToBytes(msg.id, msg.cmd, msg.args != null ? msg.args.Length : 0, msg.data != null ? msg.data.Length : 0);
                        var nowait = tcp.WriteAsync(header, 0, header.Length);
                        if (msg.args != null) nowait = tcp.WriteAsync(msg.args, 0, msg.args.Length);
                        if (msg.data != null) nowait = tcp.WriteAsync(msg.data, 0, msg.data.Length);
                    }
                    ready = sendReady.WaitAsync();
                }
            }
            catch
            {
                Aborted = true;
                tcsStop.TrySetResult(true);
            }
        }

        async Task ReceiveLoop()
        {
            try
            {
                var stream = tcp.GetStream();
                int id, cmd, argsLength, dataLength;
                var readBytes = tcp.ReadAsync(HeaderSize);
                var stop = tcsStop.Task;
                while (await Task.WhenAny(readBytes, stop) == readBytes)
                {
                    if (readBytes.Result == null) // Connection was closed.
                    {
                        var nowait = Stop();
                        break;
                    }

                    MsgHeaderFromBytes(readBytes.Result, out id, out cmd, out argsLength, out dataLength);
                    var msg = new Message { id = id, cmd = cmd };
                    if (argsLength > 0)
                    {
                        msg.args = await tcp.ReadAsync(argsLength);
                    }
                    if (dataLength > 0)
                    {
                        msg.data = await tcp.ReadAsync(dataLength);
                    }
                    if (msg.cmd == EndConnectionCommand)
                    {
                        var nowait = Stop();
                        SendResponse(msg.id, 0, null);
                        break;
                    }
                    else if (msg.cmd != KeepAliveCommand) // Ignore keepalive.
                    {
                        TaskCompletionSource<Message> tcs;
                        if (!recvMap.TryRemove(id, out tcs))
                        {
                            // This is not a response, handle the message.
                            if (msg.cmd != KeepAliveCommand)
                            {
                                var nowait = messageHandler(this, msg);
                            }
                            else
                            {
                                SendResponse(msg.id, KeepAliveResponse, null);
                            }
                        }
                        else
                        {
                            // This is a response to a prior message.
                            tcs.TrySetResult(msg);
                        }
                    }
                    readBytes = tcp.ReadAsync(HeaderSize);
                }

                var tcsCancelled = recvMap.Values.ToArray();
                recvMap.Clear();
                foreach (var tcsCancel in tcsCancelled) tcsCancel.TrySetCanceled();
            }
            catch
            {
                Aborted = true;
                tcsStop.TrySetResult(true);
            }
        }

        public Task<Message> SendMessage(int cmd, byte[] data, byte[] args)
        {
            var id = Interlocked.Increment(ref nextId);
            if (id > int.MaxValue - 1000000) Interlocked.Exchange(ref nextId, 0);

            var tcs = new TaskCompletionSource<Message>();
            recvMap[id] = tcs;
            sendQueue.Enqueue(new Message { id = id, cmd = cmd, data = data, args = args });
            sendReady.Release();
            return tcs.Task;
        }

        public void SendResponse(int respondToId, int cmd, byte[] data)
        {
            sendQueue.Enqueue(new Message { id = respondToId, cmd = cmd, data = data });
            sendReady.Release();
        }

        public Task<Message> SendKeepAlive()
        {
            return SendMessage(KeepAliveCommand, null, null);
        }

        public Task EndConnection()
        {
            return SendMessage(EndConnectionCommand, null, null);
        }
    }

    public static class TcpExtentions
    {
        public static async Task<int> ReadAsync(this TcpClient tcp, byte[] buffer, int offset, int count)
        {
            try
            {
                var stream = tcp.GetStream();
                if (tcp.ReceiveTimeout <= 0) return await stream.ReadAsync(buffer, offset, count);

                var cts = new CancellationTokenSource();
                var timeout = Task.Delay(tcp.ReceiveTimeout, cts.Token);
                var read = stream.ReadAsync(buffer, offset, count);
                if (await Task.WhenAny(read, timeout) == read)
                {
                    cts.Cancel();
                    return read.Result;
                }
                return 0;
            }
            catch
            {
                return 0;
            }
        }

        public static async Task<byte[]> ReadAsync(this TcpClient tcp, int count)
        {
            var bytes = new byte[count];
            var bytesRead = 0;
            while (bytesRead < count)
            {
                var countRead = await tcp.ReadAsync(bytes, bytesRead, count - bytesRead);
                if (countRead <= 0) return null; // Connection was closed.
                bytesRead += countRead;
            }
            return bytes;
        }

        public static async Task WriteAsync(this TcpClient tcp, byte[] buffer, int offset, int count)
        {
            var stream = tcp.GetStream();
            if (tcp.SendTimeout <= 0)
            {
                await stream.WriteAsync(buffer, offset, count);
                return;
            }

            var cts = new CancellationTokenSource();
            var timeout = Task.Delay(tcp.SendTimeout, cts.Token);
            var write = stream.WriteAsync(buffer, offset, count);
            if (await Task.WhenAny(write, timeout) == write)
            {
                cts.Cancel();
                return;
            }
            throw new TimeoutException();
        }
    }

    public class Message
    {
        public int id;
        public int cmd;
        public byte[] args;
        public byte[] data;
    }
}
