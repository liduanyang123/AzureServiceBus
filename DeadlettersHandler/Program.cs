using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DeadlettersHandler
{
    class Program
    {
        const string ServiceBusConnectionString = "Endpoint=sb://omcecommercebus.servicebus.chinacloudapi.cn/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=hhAfeflFOiq5QDBjld1R0vFHSWTjYXKqeMhr3AeCQ9o=";
        const string QueueName = "ecommerceQueue";
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            PickUpAndFixDeadletters().GetAwaiter().GetResult();
            Console.ReadLine();
        }
        static async Task PickUpAndFixDeadletters()
        {
            var cts = new CancellationTokenSource();
            var sender = new MessageSender(ServiceBusConnectionString, QueueName);
            var doneReceiving = new TaskCompletionSource<bool>();

            // here, we create a receiver on the Deadletter queue
            var dlqReceiver = new MessageReceiver(ServiceBusConnectionString, EntityNameHelper.FormatDeadLetterPath(QueueName), ReceiveMode.PeekLock);

            // close the receiver and factory when the CancellationToken fires 
            cts.Token.Register(
                async () =>
                {
                    await dlqReceiver.CloseAsync();
                    doneReceiving.SetResult(true);
                });

            // register the RegisterMessageHandler callback
            dlqReceiver.RegisterMessageHandler(
                async (message, cancellationToken1) =>
                {
                    // 找到死性消息并且再次执行消息发送
                    var resubmitMessage = message.Clone();
                    // if the cloned message has an "error" we know the main loop
                    // can't handle, let's fix the message
                    if (resubmitMessage.Label != null && resubmitMessage.Label.Equals("Scientist"))
                    {
                        resubmitMessage.Label = "Scientist";
                        await sender.SendAsync(resubmitMessage);
                    }
                    //拾取消息并且从死性质队里面删除
                    await dlqReceiver.CompleteAsync(message.SystemProperties.LockToken);
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
