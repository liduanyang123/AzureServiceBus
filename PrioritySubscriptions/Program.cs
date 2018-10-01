using Microsoft.Azure.ServiceBus;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace PrioritySubscriptions
{

    class Program
    {
        const string ServiceBusConnectionString = "Endpoint=sb://omcecommercebus.servicebus.chinacloudapi.cn/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=hhAfeflFOiq5QDBjld1R0vFHSWTjYXKqeMhr3AeCQ9o=";
        const string TopicName = "ecommerceTopic";
        static void Main(string[] args)
        {
            try
            {
                // Start senders and receivers:
                Console.WriteLine("\nLaunching senders and receivers...");


                var topicClient = new TopicClient(ServiceBusConnectionString, TopicName);

                Console.WriteLine("Preparing to send messages to {0}...", topicClient.Path);

                // Send messages to queue:
                Console.WriteLine("Sending messages to topic {0}", topicClient.Path);

                var rand = new Random();
                for (var i = 0; i < 100; ++i)
                {
                    var msg = new Message()
                    {
                        TimeToLive = TimeSpan.FromMinutes(2),
                        UserProperties =
                    {
                        { "Priority", rand.Next(1, 4) }
                    }
                    };

                    topicClient.SendAsync(msg);

                }

                Console.WriteLine();


                // All messages sent
                Console.WriteLine("\nSender complete.");

                // start receive
                Console.WriteLine("Receiving messages by priority ...");
                var subClient1 = new Microsoft.Azure.ServiceBus.SubscriptionClient(ServiceBusConnectionString,
                    TopicName, "Priority1Subscription", ReceiveMode.PeekLock);
                var subClient2 = new Microsoft.Azure.ServiceBus.SubscriptionClient(ServiceBusConnectionString,
                    TopicName, "Priority2Subscription", ReceiveMode.PeekLock);
                var subClient3 = new Microsoft.Azure.ServiceBus.SubscriptionClient(ServiceBusConnectionString,
                    TopicName, "PriorityGreaterThan2Subscription", ReceiveMode.PeekLock);


                Func<SubscriptionClient, Message, CancellationToken, Task> callback = (c, message, ct) =>
                {
                    return Task.CompletedTask;
                };

                subClient1.RegisterMessageHandler((m, c) => callback(subClient1, m, c),
                    new MessageHandlerOptions(LogMessageHandlerException) { MaxConcurrentCalls = 10, AutoComplete = true });
                subClient2.RegisterMessageHandler((m, c) => callback(subClient1, m, c),
                    new MessageHandlerOptions(LogMessageHandlerException) { MaxConcurrentCalls = 5, AutoComplete = true });
                subClient3.RegisterMessageHandler((m, c) => callback(subClient1, m, c),
                    new MessageHandlerOptions(LogMessageHandlerException) { MaxConcurrentCalls = 1, AutoComplete = true });

                Task.WaitAny(
                  Task.Run(() => Console.ReadKey()),
                  Task.Delay(TimeSpan.FromSeconds(10)));

                Task.WhenAll(subClient1.CloseAsync(), subClient2.CloseAsync(), subClient3.CloseAsync());

            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());

            }
        }
        public async Task Run()
        {

            // Start senders and receivers:
            Console.WriteLine("\nLaunching senders and receivers...");


            var topicClient = new TopicClient(ServiceBusConnectionString, TopicName);

            Console.WriteLine("Preparing to send messages to {0}...", topicClient.Path);

            // Send messages to queue:
            Console.WriteLine("Sending messages to topic {0}", topicClient.Path);

            var rand = new Random();
            for (var i = 0; i < 100; ++i)
            {
                var msg = new Message()
                {
                    TimeToLive = TimeSpan.FromMinutes(2),
                    UserProperties =
                    {
                        { "Priority", rand.Next(1, 4) }
                    }
                };

                await topicClient.SendAsync(msg);

            }

            Console.WriteLine();


            // All messages sent
            Console.WriteLine("\nSender complete.");

            // start receive
            Console.WriteLine("Receiving messages by priority ...");
            var subClient1 = new Microsoft.Azure.ServiceBus.SubscriptionClient(ServiceBusConnectionString,
                TopicName, "Priority1Subscription", ReceiveMode.ReceiveAndDelete);
            var subClient2 = new Microsoft.Azure.ServiceBus.SubscriptionClient(ServiceBusConnectionString,
                TopicName, "Priority2Subscription", ReceiveMode.ReceiveAndDelete);
            var subClient3 = new Microsoft.Azure.ServiceBus.SubscriptionClient(ServiceBusConnectionString,
                TopicName, "PriorityGreaterThan2Subscription", ReceiveMode.ReceiveAndDelete);


            Func<SubscriptionClient, Message, CancellationToken, Task> callback = (c, message, ct) =>
            {
                return Task.CompletedTask;
            };

            subClient1.RegisterMessageHandler((m, c) => callback(subClient1, m, c),
                new MessageHandlerOptions(LogMessageHandlerException) { MaxConcurrentCalls = 10, AutoComplete = true });
            subClient2.RegisterMessageHandler((m, c) => callback(subClient1, m, c),
                new MessageHandlerOptions(LogMessageHandlerException) { MaxConcurrentCalls = 5, AutoComplete = true });
            subClient3.RegisterMessageHandler((m, c) => callback(subClient1, m, c),
                new MessageHandlerOptions(LogMessageHandlerException) { MaxConcurrentCalls = 1, AutoComplete = true });

            Task.WaitAny(
              Task.Run(() => Console.ReadKey()),
              Task.Delay(TimeSpan.FromSeconds(10)));

            await Task.WhenAll(subClient1.CloseAsync(), subClient2.CloseAsync(), subClient3.CloseAsync());
        }

        static async Task LogMessageHandlerException(ExceptionReceivedEventArgs e)
        {
            Console.WriteLine("Exception: \"{0}\" {0}", e.Exception.Message, e.ExceptionReceivedContext.EntityPath);
        }
    }
}
