require('dotenv').config();
const amqp = require('amqplib');
const EventEmitter = require('events');
const uuid = require('uuid');

const connUrl = process.env.RMQ_URL;
const queue = process.env.RMQ_PRODUCER_QUEUE;
const replyQueue = 'amq.rabbitmq.reply-to';

/**
 * SEND RPC MESSAGE
 * 
 * @param channel RabbitMQ channel instance
 * @param message Message
 * @param queue Queue name
 *  
 * @return Replied Message 
 */
const sendMessage = (channel, message, queue) => {
    return new Promise(resolve => {
        const correlationId = uuid.v4();
        channel.responseEmitter.once(correlationId, resolve);
        channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)), {
            correlationId,
            replyTo: replyQueue,
        });
    });
}

(async () => {
    let connection;
    try {
        connection = await amqp.connect(connUrl);
        const channel = await connection.createChannel();
        channel.responseEmitter = new EventEmitter();
        channel.responseEmitter.setMaxListeners(0);
        channel.consume(replyQueue, message => {
            channel.responseEmitter.emit(
                message.properties.correlationId,
                message.content.toString('utf8'),
            );
        }, { noAck: true });
        console.log("[x] Producer started");

        // Init queue
        await channel.assertQueue(queue, { durable: false });

        /**
         * NESTJS MESSAGE FORMAT
         * 
         * @param pattern String of NestJS MessagePattern handler
         * @param data The message payload
         * @param id Unique message ID
         * 
         */
        const id = uuid.v4();
        const message = {
            pattern: 'request-something-to-nestjs-service',
            data: {
                item: 'book',
                time: Date.now()
            },
            id: id
        };
        const respone = await sendMessage(channel, message, queue);

        console.log("[x] Sent '%s'", JSON.stringify(message));
        console.log("[x] Received '%s'", JSON.stringify(respone));

        await channel.close();
    } catch (err) {
        console.error(err);
    } finally {
        if (connection) await connection.close();
    }
})();