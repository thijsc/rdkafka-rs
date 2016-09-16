use std::ffi::CString;
use std::io;
use std::ptr;
use std::os::raw::c_void;

use rdkafka_sys::bindings;

use super::KafkaResponseError;
use super::config::KafkaConfig;
use super::message::KafkaMessage;

#[derive(Debug,PartialEq)]
pub enum ProduceError {
    QueueFull,
    MessageSizeTooLarge,
    UnknownPartition,
    UnknownTopic,
    UnknownCause
}

#[derive(Debug,PartialEq)]
pub struct KafkaProducer {
    config: KafkaConfig, // config needs to stick around for the lifetime
    inner: *mut bindings::rd_kafka_t
}

impl KafkaProducer {
    pub fn new(config: KafkaConfig) -> Result<KafkaProducer, String> {
        let dest_size = 1024;
        let err_ptr = unsafe { CString::from_vec_unchecked(vec![0; dest_size]).into_raw() };

        let inner = unsafe {
            bindings::rd_kafka_new(
                bindings::rd_kafka_type_t::RD_KAFKA_PRODUCER,
                config.inner,
                err_ptr,
                dest_size
            )
        };

        let err = unsafe { CString::from_raw(err_ptr) };

        if inner.is_null() {
            Err(err.to_string_lossy().to_string())
        } else {
            Ok(KafkaProducer {
                config: config,
                inner: inner
            })
        }
    }

    /// Asynchronously produce a message
    /// Payload and key content will be copied and message will be added to queue.
    /// TODO: See if we can reliably use RD_KAFKA_MSG_F_FREE with a Rust Vector to avoid copying.
    pub fn produce(
        &self,
        topic: &str,
        mut payload: &[u8],
        mut key: Option<&[u8]>,
        partition: Option<i32>
    ) -> Result<(), ProduceError> {
        // TODO Make helper with error handling for topic
        let topic = unsafe {
            bindings::rd_kafka_topic_new(
                self.inner,
                CString::new(topic).expect("Failed to convert topic into CString").into_raw(),
                ptr::null_mut()
            )
        };
        assert!(!topic.is_null());

        let payload_len = payload.len();
        let key_len = match key {
            Some(ref k) => k.len(),
            None => 0
        };

        let result = unsafe {
            bindings::rd_kafka_produce(
                topic,
                partition.unwrap_or(super::RD_KAFKA_PARTITION_UA),
                super::RD_KAFKA_MSG_F_COPY,
                payload.as_ptr() as *mut c_void,
                payload_len,
                match key {
                    Some(ref mut k) => k.as_ptr() as *mut c_void,
                    None => ptr::null_mut()
                },
                key_len,
                ptr::null_mut()
            )
        };

        unsafe { bindings::rd_kafka_topic_destroy(topic) }

        if result == 0 {
            Ok(())
        } else {
            match io::Error::last_os_error().raw_os_error() {
                e if e == Some(bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__QUEUE_FULL as i32) => Err(ProduceError::QueueFull),
                e if e == Some(bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE as i32) => Err(ProduceError::MessageSizeTooLarge),
                e if e == Some(bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION as i32) => Err(ProduceError::UnknownPartition),
                e if e == Some(bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC as i32) => Err(ProduceError::UnknownTopic),
                Some(_) | None => Err(ProduceError::UnknownCause)
            }
        }
    }

    pub fn queue_length(&self) -> i32 {
        unsafe { bindings::rd_kafka_outq_len(self.inner) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::config::*;

    #[test]
    fn test_produce() {
        let producer = KafkaProducer::new(KafkaConfig::new()).expect("Producer creation should succeed");
        assert_eq!(producer.queue_length(), 0);
        producer.produce("test_topic", &[1], None, None).expect("Producing should succeed");
        producer.produce("test_topic", &[1], Some(&[1]), Some(1)).expect("Producing should succeed");
        assert_eq!(producer.queue_length(), 2);
    }
}
