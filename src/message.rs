use std::borrow::Cow;
use std::ffi::CStr;
use std::ptr;
use std::slice;
use std::os::raw::c_void;

use rdkafka_sys::bindings;

use super::KafkaResponseError;

// TODO: How to handle no key?

pub struct KafkaMessage {
    // If we created this message ourselves we need to
    // keep track of it so it doesn't get dropped.
    _inner: Option<bindings::rd_kafka_message_t>,
    inner_ptr: *mut bindings::rd_kafka_message_t,
    from_rd_message: bool,
    // Key and payload can be added so we can make sure it's
    // contents don't get dropped during the lifetime of this
    // message.
    _key: Option<Vec<u8>>,
    _payload: Option<Vec<u8>>
}

impl KafkaMessage {
    pub fn from_rd_message(inner: *mut bindings::rd_kafka_message_t) -> KafkaMessage {
        assert!(!inner.is_null());
        KafkaMessage {
            _inner: None,
            inner_ptr: inner,
            from_rd_message: true,
            _key: None,
            _payload: None
        }
    }

    pub fn new(mut key: Vec<u8>, mut payload: Vec<u8>) -> KafkaMessage {
        let mut inner = bindings::rd_kafka_message_t{
            err: bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR,
            rkt: ptr::null_mut(),
            partition: super::RD_KAFKA_PARTITION_UA,
            payload: payload.as_mut_ptr() as *mut c_void,
            len: payload.len(),
            key: key.as_mut_ptr() as *mut c_void,
            key_len: key.len(),
            offset: 0,
            _private: ptr::null_mut()
        };

        KafkaMessage {
            _inner: Some(inner),
            inner_ptr: &mut inner,
            from_rd_message: false,
            _key: Some(key),
            _payload: Some(payload)
        }
    }

    pub fn error(&self) -> KafkaResponseError {
        unsafe { KafkaResponseError::new((*self.inner_ptr).err) }
    }

    pub fn topic<'a>(&'a self) -> Option<Cow<'a, str>> {
        if unsafe { (*self.inner_ptr) }.rkt.is_null() {
            None
        } else {
            let name_cstr = unsafe {
                let name_ptr = bindings::rd_kafka_topic_name((*self.inner_ptr).rkt);
                CStr::from_ptr(name_ptr)
            };
            Some(name_cstr.to_string_lossy())
        }
    }

    pub fn partition(&self) -> i32 {
        unsafe { (*self.inner_ptr).partition }
    }

    pub fn offset(&self) -> i64 {
        unsafe { (*self.inner_ptr).offset }
    }

    pub fn key(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts((*self.inner_ptr).key as *mut u8, (*self.inner_ptr).key_len)
        }
    }

    pub fn payload(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts((*self.inner_ptr).payload as *mut u8, (*self.inner_ptr).len)
        }
    }
}

impl Drop for KafkaMessage {
    fn drop(&mut self) {
        if self.from_rd_message {
            // This was created on the C side, so we need to clean it using
            // the destroy function.
            unsafe { bindings::rd_kafka_message_destroy(self.inner_ptr) }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::ffi::CString;
    use std::os::raw::c_void;
    use std::ptr;
    use super::*;
    use rdkafka_sys::bindings;

    #[test]
    fn test_new() {
        let message = KafkaMessage::new(vec![0, 1, 2], vec![3, 4, 5, 6]);

        assert!(message.topic().is_none());
        assert!(!message.error().is_error());
        assert_eq!(-1, message.partition());
        assert_eq!(-1, message.offset());
        assert_eq!([0, 1, 2], message.key());
        assert_eq!([3, 4, 5, 6], message.payload());
    }

    #[test]
    fn test_from_rd_message() {
        let mut key = [0, 1, 2];
        let mut payload = [3, 4, 5, 6];

        let kafka = unsafe {
            bindings::rd_kafka_new(
                bindings::rd_kafka_type_t::RD_KAFKA_CONSUMER,
                bindings::rd_kafka_conf_new(),
                ptr::null_mut(),
                0
            )
        };
        let topic = unsafe {
            bindings::rd_kafka_topic_new(
                kafka,
                CString::new("topic_name").unwrap().into_raw(),
                ptr::null_mut()
            )
        };

        let mut inner = bindings::rd_kafka_message_t{
            err: bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR,
            rkt: topic,
            partition: 15,
            payload: payload.as_mut_ptr() as *mut c_void,
            len: payload.len(),
            key: key.as_mut_ptr() as *mut c_void,
            key_len: key.len(),
            offset: 100,
            _private: ptr::null_mut()
        };

        let mut message = KafkaMessage::from_rd_message(&mut inner);
        // Set to false otherwise we will run cleanup on stuff
        // the C lib didn't create.
        message.from_rd_message = false;

        assert_eq!(Some(Cow::Borrowed("topic_name")), message.topic());
        assert!(!message.error().is_error());
        assert_eq!(15, message.partition());
        assert_eq!(100, message.offset());
        assert_eq!(key, message.key());
        assert_eq!(payload, message.payload());
    }
}
