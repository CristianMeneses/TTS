using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace TtsWorker.Services;

// Punto único de acceso a RabbitMQ para el worker .NET:
//   · publica tareas de síntesis  → tts.tasks
//   · publica mensajes al marcador → dialer.output
//   · consume resultados           ← tts.results
// Una sola conexión; un canal dedicado para publicar (serializado con un semáforo,
// porque IChannel no es thread-safe) y canales aparte para consumir.
sealed class RabbitBus : IAsyncDisposable
{
    readonly IConnection _conn;
    readonly IChannel _pub;
    readonly ILogger _log;
    readonly SemaphoreSlim _pubLock = new(1, 1);

    public string TasksQueue   { get; }
    public string ResultsQueue { get; }
    public string DialerQueue  { get; }
    public string InputQueue   { get; }

    RabbitBus(IConnection conn, IChannel pub, ILogger log, string tasks, string results, string dialer, string input)
    {
        _conn = conn;
        _pub = pub;
        _log = log;
        TasksQueue = tasks;
        ResultsQueue = results;
        DialerQueue = dialer;
        InputQueue = input;
    }

    public static async Task<RabbitBus> CreateAsync(IConfiguration config, ILogger logger, CancellationToken ct = default)
    {
        var factory = new ConnectionFactory
        {
            HostName    = config["RabbitMq:Host"] ?? "localhost",
            Port        = config.GetValue("RabbitMq:Port", 5672),
            UserName    = config["RabbitMq:User"] ?? "guest",
            Password    = config["RabbitMq:Password"] ?? "guest",
            VirtualHost = config["RabbitMq:VirtualHost"] ?? "/",
        };

        var tasks   = config["Queues:Tasks"]   ?? "tts.tasks";
        var results = config["Queues:Results"] ?? "tts.results";
        var dialer  = config["Queues:Dialer"]  ?? "dialer.output";
        var input   = config["Queues:Input"]   ?? "campaigns.input";

        // Retry: RabbitMQ (Docker) puede tardar en estar listo tras arrancar.
        IConnection? conn = null;
        for (int attempt = 1; attempt <= 10 && conn is null; attempt++)
        {
            try
            {
                conn = await factory.CreateConnectionAsync(ct);
            }
            catch (Exception ex) when (attempt < 10)
            {
                logger.LogWarning("RabbitMQ no disponible (intento {N}/10): {Msg}. Reintentando en 3s…", attempt, ex.Message);
                await Task.Delay(3000, ct);
            }
        }
        if (conn is null) throw new InvalidOperationException("No se pudo conectar a RabbitMQ tras 10 intentos.");

        var pub = await conn.CreateChannelAsync(cancellationToken: ct);
        foreach (var q in new[] { tasks, results, dialer, input })
            await pub.QueueDeclareAsync(q, durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);

        logger.LogInformation("RabbitBus conectado. Colas: {Tasks}, {Results}, {Dialer}, {Input}",
            tasks, results, dialer, input);

        return new RabbitBus(conn, pub, logger, tasks, results, dialer, input);
    }

    // Publica un cuerpo JSON a una cola (vía default exchange, routingKey = nombre de cola).
    public async Task PublishAsync(string queue, ReadOnlyMemory<byte> body, CancellationToken ct = default)
    {
        var props = new BasicProperties
        {
            ContentType  = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
        };
        await _pubLock.WaitAsync(ct);
        try
        {
            await _pub.BasicPublishAsync("", queue, mandatory: false, basicProperties: props, body: body, cancellationToken: ct);
        }
        finally
        {
            _pubLock.Release();
        }
    }

    // Crea un consumidor sobre una cola. El handler recibe el cuerpo; si no lanza,
    // se hace ack; si lanza, se hace nack (sin requeue → evita poison-loop).
    public async Task<IChannel> ConsumeAsync(
        string queue,
        Func<ReadOnlyMemory<byte>, Task> handler,
        ushort prefetch,
        CancellationToken ct = default)
    {
        var channel = await _conn.CreateChannelAsync(cancellationToken: ct);
        await channel.BasicQosAsync(0, prefetch, global: false, cancellationToken: ct);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            try
            {
                await handler(ea.Body);
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Error procesando mensaje de {Queue} — descartado (nack sin requeue)", queue);
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
            }
        };

        await channel.BasicConsumeAsync(queue, autoAck: false, consumer: consumer, cancellationToken: ct);
        return channel;
    }

    public async ValueTask DisposeAsync()
    {
        try { await _pub.CloseAsync(); } catch { /* ignore */ }
        try { await _conn.CloseAsync(); } catch { /* ignore */ }
        _pub.Dispose();
        _conn.Dispose();
        _pubLock.Dispose();
    }
}
