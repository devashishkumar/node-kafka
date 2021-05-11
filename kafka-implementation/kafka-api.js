module.exports = function (app, kafka) {
    topicMessages = [];
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
        getTopicMessages(topicName, res);
        // return getTopicMessages(topicName).then(resp => {
        //     return res.send(resp);
        // })
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
    const getTopicMessages = async (topicName, respObj) => {
        console.log(topicName);
        try {
            // await consumer.connect()
            // await consumer.subscribe({ topic: topicName, fromBeginning: true })
            // await consumer.run({
            //     eachMessage: async ({ topic, partition, message }) => {
            //         console.log({
            //             value: message.value.toString()
            //         })
            //     },
            // })
            runConsumer(topicName);
            console.log(this.topicMessages, '80');

        } catch (e) {
            console.log('error');
        }
        // console.log(this.topicMessages, '114');
        // return respObj.send({ data: this.topicMessages });
    };
    // get topic messages end

    const runConsumer = async (topicName) => {

        await consumer.connect()
        await consumer.subscribe({ topic: topicName, fromBeginning: true })

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                this.topicMessages.push({
                    partition,
                    offset: message.offset,
                    value: message.value.toString(),
                })
                console.log(this.topicMessages);
            },
        })
    }
}