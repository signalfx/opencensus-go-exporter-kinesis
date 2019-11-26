var AWS = require('aws-sdk')

AWS.config.update({region: 'us-east-1'})

var kinesis = new AWS.Kinesis({endpoint: 'http://localhost:4567'})

kinesis.createStream({
    ShardCount: '1',
    StreamName: 'in_dev_test_tenant',
}, function(err, data){
    console.log(err);
    console.log(data);
})

function sleep(i) {
var waitTill = new Date(new Date().getTime() + i * 1000);
while(waitTill > new Date()){}
}

sleep(3)
