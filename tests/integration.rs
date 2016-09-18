extern crate rdkafka;

use std::borrow::Cow;
use std::time::Duration;
use std::thread;

use rdkafka::config::KafkaConfig;
use rdkafka::consumer::KafkaConsumer;
use rdkafka::producer::KafkaProducer;

// A Zookeeper and Kafka broker need to be running
// locally for these tests to pass.

#[test]
fn test_produce_and_consume_with_key() {
    let mut config = KafkaConfig::new();
    config.set("metadata.broker.list", "localhost:9092").expect("Setting broker list should not fail");
    config.set("group.id", "integration-test-consumer").expect("Setting group.id should not fail");
    config.set("enable.auto.commit", "true").expect("Setting enable.auto.commit	should not fail");

    let producer = KafkaProducer::new(config.clone()).expect("Creating producer should not fail");
    let mut consumer = KafkaConsumer::new(config).expect("Creating consumer should not fail");
    consumer.subscribe(&["integration_test"]).expect("Subscribing should not fail");
    // Poll once so we have an offset before producing
    for i in 0..10 {
        match consumer.poll() {
            Some(_) => break,
            None => {
                if i == 10 {
                    panic!("Polling beforehand failed");
                }
            }
        }
    }

    // Produce some messages
    for _ in 0..5 {
        producer.produce("integration_test", b"payload", Some(b"key"), None).expect("Producing should not fail");
    }
    assert!(producer.queue_length() > 0);

    // Wait for queue to be flushed
    for _ in 0..10 {
        if producer.queue_length() == 0 {
            break
        } else {
            thread::sleep(Duration::from_millis(250));
        }
    }
    assert_eq!(producer.queue_length(), 0);

    // Consume these messages
    for _ in 0..5 {
        let message = consumer.next().expect("Should return result").expect("Should return message");
        assert_eq!(message.topic(), Some(Cow::Borrowed("integration_test")));
        assert!(message.partition() > 0);
        assert!(message.offset() > 0);
        assert_eq!(message.payload(), b"payload");
        assert_eq!(message.key(), b"key");
    }
}

//#[test]
//fn test_consume_no_messages() {
//    let mut config = KafkaConfig::new();
//    config.set("metadata.broker.list", "localhost:9092").expect("Setting broker list should not fail");
//    config.set("group.id", "test").expect("Setting group.id should not fail");
//
//    let mut consumer = KafkaConsumer::new(config).expect("Creating consumer should not fail");
//    consumer.subscribe(&["integration_test_empty"]).expect("Subscribing should not fail");
//
//    let message = consumer.next().expect("Should return result");
//    assert!(message.is_err());
//}
