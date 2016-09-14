extern crate bindgen;

use std::env;
use std::path::Path;
use std::process::Command;
use std::fs::{File,remove_file};

static VERSION: &'static str = "0.9.1"; // Should be the same major version as in the manifest

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let build_dir = format!("{}/librdkafka-build-{}", out_dir, VERSION);
    let src_path = format!("{}/librdkafka-{}", out_dir, VERSION);
    let librdkafka_path = Path::new(&build_dir).join("lib/librdkafka.a");

    if !librdkafka_path.exists() {
        // Download and extract archive
        let url = format!("https://github.com/edenhill/librdkafka/archive/{}.tar.gz", VERSION);
        assert!(Command::new("curl").arg("-O") // Save to disk
                                    .arg("-L") // Follow redirects
                                    .arg(url)
                                    .current_dir(&out_dir)
                                    .status()
                                    .expect("Failed to download")
                                    .success());

        let archive_name = format!("{}.tar.gz", VERSION);
        assert!(Command::new("tar").arg("xzf")
                                   .arg(&archive_name)
                                   .current_dir(&out_dir)
                                   .status()
                                   .expect("Failed to unarchive")
                                   .success());

        // Configure and install
        let mut command = Command::new("bash");
        command.arg("configure");
        command.arg("--enable-ssl");
        command.arg("--enable-static");
        command.arg(format!("--prefix={}", &build_dir));
        command.current_dir(&src_path);
        // Use target that Cargo sets
        if let Ok(target) = env::var("TARGET") {
            command.arg(format!("--build={}", target));
        }
        assert!(command.status().expect("Failed to configure").success());
        assert!(Command::new("make").
                         current_dir(&src_path).
                         status().
                         expect("Failed to make").
                         success());

        assert!(Command::new("make").
                         arg("install").
                         current_dir(&src_path).
                         status().
                         expect("Failed to make install").
                         success());

        // Generate bindings
        let headers = format!("{}/src/rdkafka.h", src_path);
        let mut bindings = bindgen::Builder::new(headers);
        bindings.builtins();
        let generated_bindings = bindings.generate().expect("Failed to generate bindings");

        // Clean up bindings file
        remove_file("src/bindings.rs").unwrap_or(());

        // Write bindings to file
        let file = File::create("src/bindings.rs").expect("Failed to open file");
        generated_bindings.write(Box::new(&file)).expect("Unable to write bindings");
    }

    // Output to Cargo
    println!("cargo:rustc-link-search=native={}/lib", &build_dir);
    println!("cargo:rustc-link-lib=static=rdkafka");
}
