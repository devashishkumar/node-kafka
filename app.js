var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');

var indexRouter = require('./routes/index');
var usersRouter = require('./routes/users');

var app = express();

const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'testapp',
  // brokers: ['kafka1:9092', 'kafka2:9092']
  // brokers: ['DESKTOP-LKNKAGN.mshome.net:2181']
  brokers: ['DESKTOP-LKNKAGN.mshome.net:9092']
});

// const producer = kafka.producer()

// producer.connect();
// producer.send({
//   topic: 'test',
//   messages: [
//     { value: 'Hello KafkaJS user!' },
//   ],
// });

const producer = kafka.producer();
setInterval(() => {
  var sendMessage = async () => {
    await producer.connect()
    await producer.send({
      topic: 'test',
      messages: [
        { key: 'key1', value: 'hello world' },
        { key: 'key2', value: 'hey hey!' }
      ],
    })
    await producer.disconnect()
  }

  sendMessage();
  console.log('46');
}, 10000);

const consumer = kafka.consumer({ groupId: 'testapp' })

var receiveMessage = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic: 'test', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      })
    },
  })
};
receiveMessage();

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
