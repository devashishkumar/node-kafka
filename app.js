var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');

var indexRouter = require('./routes/index');
var usersRouter = require('./routes/users');

var app = express();
function generateRandomString() {
  return Math.random().toString(36).substring(2, 7);
}
// kafka configuration start
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'testapp',
  // brokers: ['kafka1:9092', 'kafka2:9092']
  // brokers: ['DESKTOP-LKNKAGN.mshome.net:2181']
  brokers: ['DESKTOP-LKNKAGN:9092']
});
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

// get all topics start
setInterval(() => {
  const admin = kafka.admin()
  var getAllTopics = async () => {
    await admin.connect();
    const topics = await admin.listTopics();
    return topics;
  };
  return  getAllTopics().then(allTopics => {
    console.log(allTopics);
  })
}, 5000);
// get all topics end


// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', indexRouter);
app.use('/users', usersRouter);

// catch 404 and forward to error handler
app.use(function (req, res, next) {
  next(createError(404));
});

// error handler
app.use(function (err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

module.exports = app;
