using RabbitMQ.Client;
using RabbitMqTest;

Helper helper = new Helper();
string exchange = "test_exchange";
string queue = "test_queue";
string routingKey = "sent_email";
try
{
    helper.DeclareExchange(exchange, ExchangeType.Direct);
    helper.DeclareQueue(queue, false, false);
    helper.BindQueue(queue, exchange, routingKey);
    helper.Publish(exchange, routingKey, "engin@ileriisler.com", 5, true);
    helper.BasicConsume(queue, false);
    Console.ReadLine();
}
finally
{
    helper.ConnectClose();
}




