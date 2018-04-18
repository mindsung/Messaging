using System;
using Xunit;
using MindSung.Messaging;
using System.Threading.Tasks;
using System.Net;
using System.Collections.Generic;
using System.Linq;

namespace MindSung.Test.Messaging
{
    public class MessagingTests
    {
        const int testPort = 4321;
        const int testCommand = 5;
        const int testReply = 6;

        [Fact]
        public async Task ClientServer()
        {
            using (var server = new TestServer(testPort))
            using (var client = new MessageClient(IPAddress.Parse("127.0.0.1"), testPort, 5, 0) { CommandTimeout = 200 })
            {
                await server.Start();
                var tasks = new List<Task>();
                for (var i = 0; i < 1000; i++)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        var data = new byte[] { 1, 2, 3, 4 };
                        var reply = await client.SendMessage(testCommand, data);
                        if (reply.cmd != testReply) throw new Exception("Unexpected reply.");
                        if (reply.data == null || reply.data.Length != data.Length) throw new Exception("Unexpected data length.");
                        for (var iData = 0; iData < data.Length; iData++)
                        {
                            if (reply.data[iData] != data[iData]) throw new Exception("Unexpected data.");
                        }
                    }));
                }
                await Task.WhenAll(tasks);
            }
        }

        class TestServer : MessageServer
        {
            public TestServer(int port) : base(port)
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
