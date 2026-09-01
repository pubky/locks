fn main() {
    println!(
        "cargo:rustc-env=LOCKS_PAYKIT_PATH_PREFIX={}",
        paykit_lib::PAYKIT_PATH_PREFIX
    );
}
