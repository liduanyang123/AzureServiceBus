using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
namespace SenderWithTopic
{
    class Program
    {
        const string ServiceBusConnectionString = "Endpoint=sb://omcecommercebus.servicebus.chinacloudapi.cn/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=hhAfeflFOiq5QDBjld1R0vFHSWTjYXKqeMhr3AeCQ9o=";
        const string TopicName = "ecommerceTopic";
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            for (int i = 0; i < 10; i++)
            {
                SendMessagesAsync().GetAwaiter().GetResult();
            }

            Console.ReadLine();
        }
        static async Task SendMessagesAsync()
        {
            var sender = new MessageSender(ServiceBusConnectionString, TopicName);

            dynamic data = new[]
            {
                new {name = "Einstein", firstName = "Albert"},
                new {name = "Heisenberg", firstName = "Werner"},
                new {name = "Curie", firstName = "Marie"},
                new {name = "Hawking", firstName = "Steven"},
                new {name = "Newton", firstName = "Isaac"},
                new {name = "Bohr", firstName = "Niels"},
                new {name = "Faraday", firstName = "Michael"},
                new {name = "Galilei", firstName = "Galileo"},
                new {name = "Kepler", firstName = "Johannes"},
                new {name = "Kopernikus", firstName = "Nikolaus"}
            };


            for (int i = 0; i < data.Length; i++)
            {
                var message = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data[i])))
                {
                    ContentType = "application/json",
                    Label = "Scientist",
                    MessageId = i.ToString(),
                    //TimeToLive = TimeSpan.FromMinutes(2)存活时间
                };

                await sender.SendAsync(message);
            }
        }
    }
}
