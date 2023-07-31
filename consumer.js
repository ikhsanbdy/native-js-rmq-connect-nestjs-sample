require('dotenv').config();
const amqp = require('amqplib');

const connUrl = process.env.RMQ_URL;
const queue = process.env.RMQ_CONSUMER_QUEUE;

(async () => {
    let connection;
    try {
        connection = await amqp.connect(connUrl);
        const channel = await connection.createChannel();
        console.log("[x] Consumer started");

        process.once('SIGINT', async () => {
            await channel.close();
            await connection.close();
        });

        // Init queue
        await channel.assertQueue(queue, { durable: false });

        await channel.consume(queue, (message) => {
            if (message) { // Handle message
                /**
                 * MESSAGE FORMAT FROM NESTJS
                 * 
                 * @param pattern String of NestJS MessagePattern handler
                 * @param data The message payload
                 * @param id Unique message ID
                 * 
                 * @Example
                 * {
                 *   "pattern":"request-from-nestjs-service",
                 *   "data":{"item":"pen","time":1690795229201},
                 *   "id":"b0be9c6922cd59f228354"
                 * }
                 * 
                 */
                console.log("[x] Received '%s'", JSON.stringify(JSON.parse(message.content.toString())));

                // Reply
                const response = { status: 1 };
                channel.sendToQueue(message.properties.replyTo, Buffer.from(JSON.stringify(response)), {
                    correlationId: message.properties.correlationId
                });

                console.log("[x] Reply '%s'", JSON.stringify(response));

                // Sent ack
                channel.ack(message);
            }
        });
    } catch (err) {
        console.error(err);
    }
})();