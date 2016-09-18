use std::ffi::CString;
use std::iter::Iterator;
use std::ptr;

use rdkafka_sys::bindings;

use super::KafkaResponseError;
use super::config::KafkaConfig;
use super::message::KafkaMessage;

#[derive(Debug,PartialEq)]
pub enum ConsumeError {

}

#[derive(Debug,PartialEq)]
pub struct KafkaConsumer {
    inner: *mut bindings::rd_kafka_t,
    config: KafkaConfig,
    timeout_ms: i32
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
            // Redirect the main poll queue to this consumer.
            unsafe {
                bindings::rd_kafka_poll_set_consumer(inner); // TODO error handling
            }

            Ok(KafkaConsumer {
                inner: inner,
                config: config,
                timeout_ms: 100
            })
        }
    }

    pub fn subscribe(&mut self, topics: &[&str]) -> Result<(), KafkaResponseError> {
        let topic_list = unsafe { bindings::rd_kafka_topic_partition_list_new(topics.len() as i32) };

        for topic in topics {
            unsafe {
                bindings::rd_kafka_topic_partition_list_add(
                    topic_list,
                    CString::new(*topic).expect("Converting partition name to CString failed").into_raw(),
                    super::RD_KAFKA_PARTITION_UA
                );
            }
        }

        let result = KafkaResponseError::new(unsafe { bindings::rd_kafka_subscribe(self.inner, topic_list) });

        unsafe { bindings::rd_kafka_topic_partition_list_destroy(topic_list) };

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

    /// Poll for new messages
    ///
    /// If this returns None no actual poll was executed, retry in that case.
    pub fn poll(&self) -> Option<Result<KafkaMessage, KafkaResponseError>> {
        let message_ptr = unsafe {
            bindings::rd_kafka_consumer_poll(self.inner, self.timeout_ms)
        };

        if message_ptr.is_null() {
            return None
        }

        let message = KafkaMessage::from_rd_message(message_ptr);
        if message.error().is_error() {
            Some(Err(message.error()))
        } else {
            Some(Ok(message))
        }
    }

    pub fn commit(&mut self) {
        // TODO error handling
        unsafe {
            bindings::rd_kafka_commit(
                self.inner,
                ptr::null_mut(),
                self.timeout_ms
            );
        }
    }
}

impl Iterator for KafkaConsumer {
    type Item = Result<KafkaMessage, KafkaResponseError>;

    // NEXT: Make an iterator that can be configured to end or continue at EOF
    //       This already sort of makes sense for the continue scenario. Error
    //       wrapper needs to clearer.
    fn next(&mut self) -> Option<Self::Item> {
        // Loop until we get a result from polling
        loop {
            match self.poll() {
                Some(result) => {
                    println!("RESULT: {:?}", result);
                    match result {
                        Err(err) => {
                            if !err.is_eof() {
                                return Some(Err(err))
                            }
                        },
                        Ok(message) => return Some(Ok(message))
                    }
                },
                None => ()
            }
        }
    }
}

impl Drop for KafkaConsumer {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe {
                // TODO error handling
                bindings::rd_kafka_consumer_close(self.inner);
                bindings::rd_kafka_destroy(self.inner);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::config::*;
    use super::*;

    #[test]
    fn test_new_subscribe_poll_unsubscribe() {
        let mut config = KafkaConfig::new();
        config.set("group.id", "test").expect("Setting group id should not fail");
        let mut consumer = KafkaConsumer::new(config).expect("Creating consumer should not fail");

        consumer.subscribe(&["test_topic"]).expect("Subscribing should not fail");
        consumer.poll().expect("Polling should return some").expect("Polling should not fail");
        consumer.unsubscribe().expect("Unsubscribing should not fail");
    }

    #[test]
    fn test_consume_no_group_id() {
        let config = KafkaConfig::new();
        let mut consumer = KafkaConsumer::new(config).expect("Creating consumer should not fail");

        consumer.subscribe(&["test_topic"]).expect("Subscribing should not fail");
        assert!(consumer.poll().expect("Should not be none").is_err());
    }
}
