using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using TtsWorker.Models;

namespace TtsWorker.Services;

sealed class RabbitPublisher : IAsyncDisposable
{
    readonly IConnection _conn;
    readonly IChannel _channel;
    readonly string _exchange;
    readonly string _routingKey;

    private RabbitPublisher(IConnection conn, IChannel channel, string exchange, string routingKey)
    {
        _conn = conn;
        _channel = channel;
        _exchange = exchange;
        _routingKey = routingKey;
    }

    public static async Task<RabbitPublisher> CreateAsync(
        string host, int port, string user, string password,
        string virtualHost, string exchange, string routingKey,
        string queueName)
    {
        var factory = new ConnectionFactory
        {
            HostName = host,
            Port = port,
            UserName = user,
            Password = password,
            VirtualHost = virtualHost,
        };

        var conn = await factory.CreateConnectionAsync();
        var channel = await conn.CreateChannelAsync();

        // Declara la cola y el exchange de forma idempotente
        await channel.ExchangeDeclareAsync(exchange, ExchangeType.Direct, durable: true);
        await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync(queueName, exchange, routingKey);

        return new RabbitPublisher(conn, channel, exchange, routingKey);
    }

    public async Task PublishAsync(DialerMessage message, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(message);
        var body = Encoding.UTF8.GetBytes(json);

        var props = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
        };

        await _channel.BasicPublishAsync(_exchange, _routingKey, true, props, body, ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _channel.CloseAsync();
        await _conn.CloseAsync();
        _channel.Dispose();
        _conn.Dispose();
    }
}
