using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ReceiverWithQueue
{
    class Program
    {
        const string ServiceBusConnectionString = "Endpoint=sb://omcecommercebus.servicebus.chinacloudapi.cn/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=hhAfeflFOiq5QDBjld1R0vFHSWTjYXKqeMhr3AeCQ9o=";
        const string QueueName = "ecommerceQueue";
        static IQueueClient queueClient;
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            ReceiveMessagesAsync().GetAwaiter().GetResult();
        }
        static async Task ReceiveMessagesAsync()
        {
            var cancellationToken = new CancellationToken();
            var receiver = new MessageReceiver(ServiceBusConnectionString, QueueName, ReceiveMode.PeekLock);


            var doneReceiving = new TaskCompletionSource<bool>();
            // close the receiver and factory when the CancellationToken fires 
            //个人建议不需要关闭 每次很费时才连接上
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
                        //todo  scientist
                        await receiver.CompleteAsync(message.SystemProperties.LockToken);
                    }
                    else
                    {
                        await receiver.DeadLetterAsync(message.SystemProperties.LockToken); //, "ProcessingError", "不知道要干嘛的");
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
