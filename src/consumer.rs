use std::ffi::CString;

use rdkafka_sys::bindings;

use super::config::KafkaConfig;

#[derive(Debug,PartialEq)]
pub struct KafkaConsumer {
    config: KafkaConfig, // config needs to stick around for the lifetime
    pub inner: *mut bindings::rd_kafka_t
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
}

impl Drop for KafkaConsumer {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe { bindings::rd_kafka_destroy(self.inner) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::config::*;

    #[test]
    fn test_new() {
        let config = KafkaConfig::new();
        assert!(KafkaConsumer::new(config).is_ok());
    }
}
