use std::ffi::CString;

use rdkafka_sys::bindings;

use super::KafkaResponseError;
use super::config::KafkaConfig;

#[derive(Debug,PartialEq)]
pub struct KafkaConsumer {
    config: KafkaConfig, // config needs to stick around for the lifetime
    inner: *mut bindings::rd_kafka_t
}

impl KafkaConsumer {
    pub fn new(config: KafkaConfig) -> Result<KafkaConsumer, String> {
        let dest_size = 1024;
        let err_ptr = unsafe { CString::from_vec_unchecked(vec![0; dest_size]).into_raw() };

        let inner = unsafe {
            bindings::rd_kafka_new(
                bindings::rd_kafka_type_t::RD_KAFKA_CONSUMER,
                config.inner,
                err_ptr,
                dest_size
            )
        };

        let err = unsafe { CString::from_raw(err_ptr) };

        if inner.is_null() {
            Err(err.to_string_lossy().to_string())
        } else {
            Ok(KafkaConsumer {
                config: config,
                inner: inner
            })
        }
    }

    pub fn subscribe(&mut self, partitions: &[&str]) -> Result<(), KafkaResponseError> {
        let topics = unsafe { bindings::rd_kafka_topic_partition_list_new(partitions.len() as i32) };

        for partition in partitions {
            unsafe {
                bindings::rd_kafka_topic_partition_list_add(
                    topics,
                    CString::new(*partition).expect("Converting partition name to CString failed").into_raw(),
                    super::RD_KAFKA_PARTITION_UA
                );
            }
        }

        let result = KafkaResponseError::new(unsafe { bindings::rd_kafka_subscribe(self.inner, topics) });

        unsafe { bindings::rd_kafka_topic_partition_list_destroy(topics) };

        if result.is_error() {
            Err(result)
        } else {
            Ok(())
        }
    }

    pub fn unsubscribe(&mut self) -> Result<(), KafkaResponseError> {
        let result = KafkaResponseError::new(unsafe { bindings::rd_kafka_unsubscribe(self.inner) });
        if result.is_error() {
            Err(result)
        } else {
            Ok(())
        }
    }

    pub fn pause(&mut self) {

    }

    pub fn resume(&mut self) {

    }

    pub fn poll(&self) {

    }

    pub fn commit(&mut self) {

    }
}

impl Drop for KafkaConsumer {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe {
                bindings::rd_kafka_consumer_close(self.inner);
                bindings::rd_kafka_destroy(self.inner);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::config::*;

    #[test]
    fn test_new_and_subscribe() {
        let mut config = KafkaConfig::new();
        config.set("group.id", "test").expect("Setting group id should not fail");

        let consumer_result = KafkaConsumer::new(config);
        assert!(consumer_result.is_ok());

        let mut consumer = consumer_result.unwrap();
        consumer.subscribe(&["test_topic"]).expect("Subscribing should not fail");
        consumer.unsubscribe().expect("Unsubscribing should not fail");
    }
}
