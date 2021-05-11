module.exports = function (app, kafka) {
    app.get('/kafkatopics', (req, res) => {
        return getAllTopics().then(allTopics => {
            return res.send(allTopics);
        })
    });

    app.delete('/deleteTopics', (req, res) => {
        const topic = req.body.topic;
        return deleteTopics([topic]).then(res => {
            return res.send({ status: 200, message: 'Topic Deleted Successfully' });
        })
    });

    app.get('/fetchTopicOffsets', (req, res) => {
        const topicName = req.query.topic;
        return fetchTopicOffsets(topicName.toString()).then(resp => {
            return res.send(resp);
        })
    });

    app.get('/getTopicMessages', (req, res) => {
        const topicName = req.query.topic;
        // console.log(getTopicMessages(topicName));
        // return res.send(getTopicMessages(topicName));
        return getTopicMessages(topicName).then(resp => {
            return res.send(resp);
        })
    });

    const admin = kafka.admin()
    /**
     * 
     * @returns topics list
     */
    const getAllTopics = async () => {
        await admin.connect();
        const topics = await admin.listTopics();
        return topics;
    };

    /**
     * 
     * @param topic array
     * @returns 
     */
    const deleteTopics = async (topic) => {
        await admin.connect();
        const deleteRes = await admin.deleteTopics(topic);
        return deleteRes;
    };

    /**
     * 
     * @param topicName string
     * @returns topic offsets array
     */
    const fetchTopicOffsets = async (topicName) => {
        await admin.connect();
        const topicOffsets = await admin.fetchTopicOffsets(topicName);
        return topicOffsets;
    };

    // get topic messages
    const consumer = kafka.consumer({ groupId: 'testapp' })
    const getTopicMessages = async (topicName) => {
        console.log(topicName);
        try {
            await consumer.connect()
            await consumer.subscribe({ topic: topicName, fromBeginning: true })
            // const data = [];
            // await consumer.run({
            //     eachMessage: async ({ topic, partition, message }) => {
            //         console.log({
            //             value: message.value.toString()
            //         })
            //     },
            // })
            // return await data;

            await consumer.run({
                eachBatchAutoResolve: true,
                eachBatch: async ({
                    batch,
                    resolveOffset,
                    heartbeat,
                    commitOffsetsIfNecessary,
                    uncommittedOffsets,
                    isRunning,
                    isStale,
                }) => {
                    // const data = [];
                    for (let message of batch.messages) {
                        console.log({
                            topic: batch.topic,
                            partition: batch.partition,
                            highWatermark: batch.highWatermark,
                            message: {
                                offset: message.offset,
                                key: message.key.toString(),
                                value: message.value.toString(),
                                headers: message.headers,
                            }
                        });

                        resolveOffset(message.offset)
                        await heartbeat()
                    }
                    // return await data;
                },
            })

        } catch (e) {
            console.log('error');
        }
    };
    // get topic messages end
}