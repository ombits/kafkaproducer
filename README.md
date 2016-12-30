# kafkaproducer

It tails over file and send each lines into kafka brokers

Run:

kafkaproducer --files  <log file paths with comma separted>  --kafka <kafka brokers with comma separted>  --topic <topics name>
e.g : kafkaproducer --files  /opt/historical.log  --kafka 10.16.47.144:9092,10.16.47.144:9092,10.16.47.146:9092 --topic a_test
