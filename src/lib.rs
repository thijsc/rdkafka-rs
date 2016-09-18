extern crate rdkafka_sys;

use std::ffi::CStr;
use std::fmt;

use rdkafka_sys::bindings;

pub mod config;
pub mod consumer;
pub mod error;
pub mod message;
pub mod producer;

const RD_KAFKA_PARTITION_UA: i32 = -1;
const RD_KAFKA_MSG_F_FREE: i32 = 0x1;
const RD_KAFKA_MSG_F_COPY: i32 = 0x2;

pub struct KafkaResponseError {
    inner: bindings::rd_kafka_resp_err_t
}

impl KafkaResponseError {
    pub fn new(inner: bindings::rd_kafka_resp_err_t) -> KafkaResponseError {
        KafkaResponseError {
            inner: inner
        }
    }

    pub fn is_error(&self) -> bool {
        match self.inner {
            bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR => false,
            _ => true
        }
    }

    pub fn is_eof(&self) -> bool {
        match self.inner {
            bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__PARTITION_EOF => true,
            _ => false
        }
    }
}

impl fmt::Display for KafkaResponseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let error_cow = unsafe {
            let error_ptr = bindings::rd_kafka_err2str(self.inner);
            CStr::from_ptr(error_ptr).to_string_lossy()
        };
        write!(f, "{}", error_cow)
    }
}

impl fmt::Debug for KafkaResponseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KafkaResponseError ({})", self)
    }
}

// TODO test
pub fn shutdown() -> Result<(), ()> {
    if unsafe { bindings::rd_kafka_wait_destroyed(2000) } == -1 {
        Err(())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka_sys::bindings;

    #[test]
    fn test_is_error() {
        let error = KafkaResponseError::new(bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE);
        assert!(error.is_error());

        let no_error = KafkaResponseError::new(bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR);
        assert!(!no_error.is_error());
    }

    #[test]
    fn test_kafka_response_error_display() {
        let error = KafkaResponseError::new(bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE);
        assert_eq!(format!("{}", error), "Broker: Group coordinator not available".to_owned());
    }

    #[test]
    fn test_kafka_response_errordebug() {
        let error = KafkaResponseError::new(bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE);
        assert_eq!(format!("{:?}", error), "KafkaResponseError (Broker: Group coordinator not available)".to_owned());
    }
}
