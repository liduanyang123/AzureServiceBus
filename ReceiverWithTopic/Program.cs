using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ReceiverWithTopic
{

    class Program
    {
        const string ServiceBusConnectionString = "Endpoint=sb://omcecommercebus.servicebus.chinacloudapi.cn/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=hhAfeflFOiq5QDBjld1R0vFHSWTjYXKqeMhr3AeCQ9o=";
        const string TopicName = "ecommerceTopic";
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            Run().GetAwaiter().GetResult();
            Console.ReadLine();
        }

        static async Task Run()
        {
            var cts = new CancellationTokenSource();

            var allReceives = Task.WhenAll(
                ReceiveMessagesAsync("Subscription1", cts.Token),
                ReceiveMessagesAsync("Subscription2", cts.Token),
                ReceiveMessagesAsync("Subscription3", cts.Token));

            await Task.WhenAll(
                Task.WhenAny(
                    Task.Run(() => Console.ReadKey()),
                    Task.Delay(TimeSpan.FromSeconds(10))
                ).ContinueWith((t) => cts.Cancel()),
                allReceives);
        }
        static async Task ReceiveMessagesAsync(string subscriptionName, CancellationToken cancellationToken)
        {
            // var subscriptionPath = SubscriptionClient.FormatSubscriptionPath(topicName, subscriptionName);
            //var receiver = new MessageReceiver(connectionString,subscriptionPath, ReceiveMode.PeekLock);

            var receiver = new SubscriptionClient(ServiceBusConnectionString, TopicName, subscriptionName);


            var doneReceiving = new TaskCompletionSource<bool>();
            // close the receiver and factory when the CancellationToken fires 
            cancellationToken.Register(
                async () =>
                {
                    await receiver.CloseAsync();
                    doneReceiving.SetResult(true);
                });

            // register the RegisterMessageHandler callback
            receiver.RegisterMessageHandler(
                async (message, cancellationToken1) =>
                {
                    if (message.Label != null &&
                        message.ContentType != null &&
                        message.Label.Equals("Scientist123", StringComparison.InvariantCultureIgnoreCase) &&
                        message.ContentType.Equals("application/json", StringComparison.InvariantCultureIgnoreCase))
                    {
                        var body = message.Body;

                        dynamic scientist = JsonConvert.DeserializeObject(Encoding.UTF8.GetString(body));

                        //要处理的消息
                        //todo scientist
                        await receiver.CompleteAsync(message.SystemProperties.LockToken);
                    }
                    else
                    {
                        await receiver.DeadLetterAsync(message.SystemProperties.LockToken);//, "ProcessingError", "Don't know what to do with this message");
                    }
                },
                new MessageHandlerOptions((e) => LogMessageHandlerException(e)) { AutoComplete = false, MaxConcurrentCalls = 1 });

            await doneReceiving.Task;
        }

        static Task LogMessageHandlerException(ExceptionReceivedEventArgs e)
        {
            Console.WriteLine("Exception: \"{0}\" {0}", e.Exception.Message, e.ExceptionReceivedContext.EntityPath);
            return Task.CompletedTask;
        }
    }
}
