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
        return fetchTopicOffsets(topicName.toString()).then(resp => {
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
}