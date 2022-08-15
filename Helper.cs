using System;
using System.Text;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMqTest
{
    public class Helper
    {
        private  IConnection connection;
        private IModel channel;
       
        public Helper()
        {
            this.connection = GetConnection();
            AddLog("Sunucuya bağlanıldı...");

            this.channel = connection.CreateModel();
        }

        public void DeclareExchange(string exchangeName, string exchangeType)
        {
            channel.ExchangeDeclare(exchangeName, exchangeType);
            AddLog($"exchange:{exchangeName} ve type:{exchangeType} oluşturuldu.");
        }

        public void DeclareQueue(string queueName, bool autoDelete, bool exclusive)
        {
            this.channel.QueueDeclare(
                queueName,
                true,//RabbitMQ sunucusu kapanıp açılsa bile Queue'yu tutmak için
                exclusive,
                autoDelete);
            AddLog($"queue:{queueName} oluşturuldu.");
        }

        public void BindQueue(string queueName, string exchangeName, string routingKey)
        {
            this.channel.QueueBind(queueName, exchangeName, routingKey);
            AddLog($"queue:{queueName}, exchange:{exchangeName}, routingKey:{routingKey} bağlandı.");
        }

        public void Publish(string exchangeName, string routingKey, string message, int repeat, bool useCounter)
        {
            for (int i = 0; i < repeat; i++)
            {
                var newMessage = message;

                if(useCounter)
                    newMessage = $"[{i + 1}] - {message}";

                var dataArr = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(newMessage));

                var properties = this.channel.CreateBasicProperties();
                properties.Persistent = true;//RabbitMQ sunucusu kapanıp açılsa bile message'ları tutmak için

                this.channel.BasicPublish(
                    exchangeName,
                    routingKey,
                    properties,
                    dataArr);
            }

            AddLog($"exchange:{exchangeName}, routingKey:{routingKey}, Message: {message} gönderildi.");
        }

        public void BasicConsume(string queueName, bool autoAck)
        {
            var consumerEvent = new EventingBasicConsumer(this.channel);
            consumerEvent.Received += ConsumerEvent_Received;
            this.channel.BasicConsume(queueName, autoAck, consumerEvent);
        }

        private void ConsumerEvent_Received(object? sender, BasicDeliverEventArgs e)
        {
            var byteArr = e.Body.ToArray();
            var bodyStr = Encoding.UTF8.GetString(byteArr);
            //channel.BasicAck(deliveryTag: e.DeliveryTag, multiple: false);
            AddLog($"Veriler Alındı: {bodyStr}");
        }

        public void AddLog(string logStr)
        {
            logStr = $"{DateTime.Now:G} - {logStr}";
            Console.WriteLine(logStr);
        }

        public void ConnectClose()
        {
            this.connection.Close();
        }

        private IConnection GetConnection()
        {
            ConnectionFactory factory = new ConnectionFactory()
            {
                Uri = new Uri("amqp://admin:admin@192.168.1.1:5672", UriKind.RelativeOrAbsolute)
            };
            return factory.CreateConnection();
        }

    }
}

