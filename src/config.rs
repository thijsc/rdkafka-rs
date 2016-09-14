use std::ffi::CString;

use rdkafka_sys::bindings;

#[derive(Debug,PartialEq)]
pub struct KafkaConfig {
    inner: *mut bindings::rd_kafka_conf_t
}

#[derive(Debug,PartialEq)]
pub enum ConfigError {
    Unknown(String),
    Invalid(String)
}

impl KafkaConfig {
    pub fn new() -> KafkaConfig {
        let inner = unsafe { bindings::rd_kafka_conf_new() };
        assert!(!inner.is_null());
        KafkaConfig { inner: inner }
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<(), ConfigError> {
        let key_cstring = CString::new(key).unwrap();
        let value_cstring = CString::new(value).unwrap();
        let dest_size = 1024;
        let err_ptr = unsafe { CString::from_vec_unchecked(vec![0; dest_size]).into_raw() };

        let result = unsafe {
            bindings::rd_kafka_conf_set(
                self.inner,
                key_cstring.as_ptr(),
                value_cstring.as_ptr(),
                err_ptr,
                dest_size
            )
        };

        let err = unsafe { CString::from_raw(err_ptr) };

        match result {
            bindings::rd_kafka_conf_res_t::RD_KAFKA_CONF_UNKNOWN => {
                Err(ConfigError::Unknown(err.to_string_lossy().to_string()))
            },
            bindings::rd_kafka_conf_res_t::RD_KAFKA_CONF_INVALID => {
                Err(ConfigError::Invalid(err.to_string_lossy().to_string()))
            },
            bindings::rd_kafka_conf_res_t::RD_KAFKA_CONF_OK => Ok(())
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let key_cstring = CString::new(key).unwrap();
        let mut dest_size = 1024;
        let dest_ptr = unsafe { CString::from_vec_unchecked(vec![0; dest_size]).into_raw() };

        let result = unsafe {
            bindings::rd_kafka_conf_get(
                self.inner,
                key_cstring.as_ptr(),
                dest_ptr,
                &mut dest_size
            )
        };

        let dest = unsafe { CString::from_raw(dest_ptr) };

        match result {
            bindings::rd_kafka_conf_res_t::RD_KAFKA_CONF_OK => {
                Some(dest.to_string_lossy().to_string())
            },
            _ => None
        }
    }
}

impl Drop for KafkaConfig {
    fn drop(&mut self) {
        unsafe {
            bindings::rd_kafka_conf_destroy(self.inner);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_get_and_set() {
        let mut config = KafkaConfig::new();
        assert!(config.set("group.id", "group-1").is_ok());
        assert_eq!(Some("group-1".to_owned()), config.get("group.id"));
    }

    #[test]
    fn test_set_unknown() {
        let mut config = KafkaConfig::new();
        assert_eq!(config.set("nonsense", "value"), Err(ConfigError::Unknown("No such configuration property: \"nonsense\"".to_owned())));
    }

    #[test]
    fn test_set_invalid() {
        let mut config = KafkaConfig::new();
        assert_eq!(config.set("retries", "nonsense"), Err(ConfigError::Invalid("Invalid value for configuration property \"message.send.max.retries\"".to_owned())));
    }

    #[test]
    fn test_get_not_set() {
        let config = KafkaConfig::new();
        assert!(config.get("key").is_none());
    }
}
