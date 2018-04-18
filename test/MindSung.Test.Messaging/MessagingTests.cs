using System;
using Xunit;
using MindSung.Messaging;
using System.Threading.Tasks;
using System.Net;
using System.Collections.Generic;

namespace MindSung.Test.Messaging
{
    public class MessagingTests
    {
        [Fact]
        public async Task ClientServer()
        {
            using (var server = new Server(4321))
            using (var client = new Client(IPAddress.Parse("127.0.0.1"), 4321, 5, 0))
            {
                var tasks = new List<Task>();
                await server.Start();
                for (var i = 0; i < 100; i++)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        await client.SendTestMessage();
                    }));
                }
                await Task.WhenAll(tasks);
            }
        }

        const int testCommand = 5;
        const int testReply = 6;

        class Client : MessageClient
        {
            public Client(IPAddress hostAddress, int port, int numConnections = 1, int recycleTimeMs = 0) : base(hostAddress, port, numConnections, recycleTimeMs)
            {
            }

            protected override Task OnMessage(MessageConnection connection, Message message)
            {
                return Task.CompletedTask;
            }

            public async Task SendTestMessage()
            {
                var data = new byte[] { 1, 2, 3, 4 };
                var reply = await SendMessage(testCommand, data);
                if (reply.cmd != testReply) throw new Exception("Unexpected reply.");
                if (reply.data == null || reply.data.Length != data.Length) throw new Exception("Unexpected data length.");
                for (var i = 0; i < data.Length; i++)
                {
                    if (reply.data[i] != data[i]) throw new Exception("Unexpected data.");
                }
            }
        }

        class Server : MessageServer
        {
            public Server(int port) : base(port)
            {
            }

            protected override Task OnMessage(MessageConnection connection, Message message)
            {
                if (message.cmd != testCommand) throw new Exception("Unexpected command.");
                // Echo back data
                connection.SendResponse(message.id, testReply, message.data);
                return Task.CompletedTask;
            }
        }
    }
}
