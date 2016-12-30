package main

import (
    "github.com/hpcloud/tail"
    "github.com/Shopify/sarama"
    "log"
    "os"
    "os/signal"
    "flag"
    "strings"
    "time"
)

func reader(file_name string, buffer chan string) {
    log.Print("Starting file %s", file_name)
    t, err := tail.TailFile(file_name, tail.Config{Follow:true, ReOpen:true})
    if err != nil {
        log.Panic(err)
    }
    for line := range t.Lines {
        buffer <- line.Text
    }
}

func writer(buffer chan string, kafka_workers []string, signals chan os.Signal,topic string) {
    var enqueued int = 0
    producer,err := sarama.NewAsyncProducer(kafka_workers, nil)
    if err != nil {
       panic(err)
    }
    defer func() {
            if err := producer.Close(); err != nil {
                log.Fatalln(err)
            }
    }()

    ProducerLoop:
    for {
        var val string = <- buffer
        // log.Print(val)
        msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(val)}
        select {
            case producer.Input() <- msg:
                enqueued++

            case <-signals:
                producer.AsyncClose() // Trigger a shutdown of the producer.
                break ProducerLoop
        }
    }
}

func main() {
    file_names := flag.String("files", "a.txt", "A comma separated list of files to watch")
    kafka_worker_str := flag.String("kafka", "localhost:9092", "A comma separated list of Kafka workers")
    kafka_topics := flag.String("topic", "test", "A comma separted topic lists")
    flag.Parse()
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, os.Interrupt)

    config := sarama.NewConfig()
    config.Producer.Return.Successes = true
    config.Producer.Compression = sarama.CompressionGZIP
    config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
    buffer := make(chan string, 100);

    files := strings.Split(*file_names, ",")
    kafka_workers := strings.Split(*kafka_worker_str, ",")
    kafka_topic := strings.Split(*kafka_topics, ",")
    go writer(buffer, kafka_workers, signals,kafka_topics[0])
    for _,file_name := range files {
        file_name = strings.Trim(file_name, " ")
        if _, err := os.Stat(file_name); os.IsNotExist(err) {
            log.Printf("no such file or directory: %s", file_name)
            return
        }
        go reader(file_name, buffer)
    }
    for ;; {
        time.Sleep(1000 * time.Millisecond)
    }
}

