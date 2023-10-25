.PHONY:
release:
	cargo zigbuild --release --target x86_64-unknown-linux-gnu -Z unstable-options --out-dir bin/
