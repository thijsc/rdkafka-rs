extern crate rdkafka_sys;

use rdkafka_sys::bindings;

pub mod config;

pub struct Kafka {
    inner: *mut bindings::rd_kafka_t
}

impl Drop for Kafka {
    fn drop(&mut self) {
        unsafe {
            bindings::rd_kafka_destroy(self.inner);
        }
    }
}
