// kafka configuration start
module.exports = function (kafka) {
    // const kafka = new Kafka({
    //     clientId: 'testapp',
    //     brokers: ['DESKTOP-LKNKAGN:9092']
    // });
    // kafka configuration end

    // kafka producer start
    const producer = kafka.producer();
    let newTopic = 'test';
    setInterval(() => {
        // newTopic = generateRandomString();
        var sendMessage = async () => {
            await producer.connect()
            await producer.send({
                topic: 'testnew',
                messages: [
                    { key: 'name', value: 'Ashish K' }
                ],
            })
            await producer.disconnect()
        }

        sendMessage();
    }, 10000);
    // kafka producer end

    // kafka subscriber start
    const consumer = kafka.consumer({ groupId: 'testapp' })

    var receiveMessage = async () => {
        try {
            await consumer.connect()
            await consumer.subscribe({ topic: 'testnew', fromBeginning: true })

            await consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    console.log({
                        value: message.value.toString(),
                    })
                },
            })
        } catch (e) {
            console.log('error');
        }

    };
    // kafka subscriber end
    receiveMessage();

    setInterval(() => {
        const admin = kafka.admin()
        const fetchTopicOffsets = async (topicName) => {
            await admin.connect();
            const topicOffsets = await admin.fetchTopicOffsets(topicName);
            return topicOffsets;
        };
        return fetchTopicOffsets('testnew').then(topicOffsets => {
            console.log(topicOffsets, '60');
        })
    }, 10000)

    // get all topics start
    const admin = kafka.admin()
    var getAllTopics = async () => {
        await admin.connect();
        const topics = await admin.listTopics();
        return topics;
    };
    return getAllTopics().then(allTopics => {
        console.log(allTopics);
    })
    // get all topics
}
