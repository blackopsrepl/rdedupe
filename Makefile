BINARY := rdedupe
VERSION := $(shell sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
HOST := $(shell rustc -vV | sed -n 's/^host: //p')
CARGO ?= cargo
CARGO_TARGET_DIR ?= target
PREFIX ?= $(HOME)/.cargo
BINDIR := $(PREFIX)/bin
DIST_DIR ?= dist
# Build target for release/dist; empty means the host. Static Linux binaries use musl:
#   make dist TARGET=x86_64-unknown-linux-musl   (needs musl-gcc, e.g. the musl-tools package)
TARGET ?=
TARGET_FLAG := $(if $(TARGET),--target $(TARGET),)
RELEASE_DIR := $(CARGO_TARGET_DIR)/$(if $(TARGET),$(TARGET)/,)release
PACKAGE := rdedupe-$(VERSION)-$(if $(TARGET),$(TARGET),$(HOST))
export CC_x86_64_unknown_linux_musl ?= musl-gcc
export CC_aarch64_unknown_linux_musl ?= aarch64-linux-musl-gcc

.PHONY: all build release format format-check lint test resource-tests skill-check \
	filesystem-acceptance benchmark check dist dist-check clean help build-release \
	static static-x86 static-arm install uninstall install-user uninstall-user fmt

all: check release

build:
	$(CARGO) build --locked

release:
	$(CARGO) build --release --locked $(TARGET_FLAG)

# Compatibility alias for the 2.x target name.
build-release: release

# Static Linux binaries (musl). Need a musl C compiler for the bundled SQLite.
static: static-x86

static-x86:
	$(MAKE) release TARGET=x86_64-unknown-linux-musl

static-arm:
	$(MAKE) release TARGET=aarch64-unknown-linux-musl

install: release
	install -d $(DESTDIR)$(BINDIR)
	install -m 755 $(RELEASE_DIR)/$(BINARY) $(DESTDIR)$(BINDIR)/$(BINARY)
	@echo "Installed $(BINARY) to $(DESTDIR)$(BINDIR)"

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/$(BINARY)

install-user:
	$(CARGO) install --locked --path .

uninstall-user:
	$(CARGO) uninstall $(BINARY) 2>/dev/null || true

format:
	$(CARGO) fmt --all

fmt: format

format-check:
	$(CARGO) fmt --all -- --check

lint:
	$(CARGO) clippy --locked --all-targets --all-features -- -D warnings

test:
	$(CARGO) test --locked --all-targets

resource-tests:
	$(CARGO) test --locked --test resources -- --nocapture

skill-check:
	$(CARGO) test --locked --test agent_skill --test agent_workflows

# Real FIDEDUPERANGE acceptance; set TMPDIR to a Btrfs or XFS directory to exercise it.
filesystem-acceptance:
	$(CARGO) test --locked --test extent_dedupe -- --nocapture

benchmark:
	$(CARGO) bench --locked --bench pipeline

check: format-check lint test

# Release archive: binary, license, README, docs and the version-paired agent skill.
dist: release
	rm -rf $(DIST_DIR)/$(PACKAGE) $(DIST_DIR)/$(PACKAGE).tar.gz $(DIST_DIR)/$(PACKAGE).tar.gz.sha256
	mkdir -p $(DIST_DIR)/$(PACKAGE)/skills $(DIST_DIR)/$(PACKAGE)/docs
	cp $(RELEASE_DIR)/$(BINARY) LICENSE README.md $(DIST_DIR)/$(PACKAGE)/
	cp -R skills/rdedupe $(DIST_DIR)/$(PACKAGE)/skills/
	cp docs/*.md $(DIST_DIR)/$(PACKAGE)/docs/
	tar -C $(DIST_DIR) -czf $(DIST_DIR)/$(PACKAGE).tar.gz $(PACKAGE)
	cd $(DIST_DIR) && sha256sum $(PACKAGE).tar.gz > $(PACKAGE).tar.gz.sha256

# Verifies the archive that ships: checksum, contents, and the extracted binary itself.
dist-check:
	cd $(DIST_DIR) && sha256sum -c $(PACKAGE).tar.gz.sha256
	rm -rf $(DIST_DIR)/verify && mkdir -p $(DIST_DIR)/verify
	tar -C $(DIST_DIR)/verify -xzf $(DIST_DIR)/$(PACKAGE).tar.gz
	for f in rdedupe LICENSE README.md skills/rdedupe/SKILL.md skills/rdedupe/references/commands.md \
		skills/rdedupe/references/safety.md skills/rdedupe/references/workflows.md \
		skills/rdedupe/references/recovery.md skills/rdedupe/templates/completion-report.md \
		skills/rdedupe/scripts/summarize.py docs/safety.md docs/formats.md docs/cli.md; do \
		test -f $(DIST_DIR)/verify/$(PACKAGE)/$$f || { echo "missing $$f"; exit 1; }; done
	$(DIST_DIR)/verify/$(PACKAGE)/rdedupe --version | grep -qx "rdedupe $(VERSION)"
	grep -q 'version: "$(VERSION)"' $(DIST_DIR)/verify/$(PACKAGE)/skills/rdedupe/SKILL.md
	mkdir -p $(DIST_DIR)/verify/tree && printf same > $(DIST_DIR)/verify/tree/a && printf same > $(DIST_DIR)/verify/tree/b
	$(DIST_DIR)/verify/$(PACKAGE)/rdedupe scan $(DIST_DIR)/verify/tree --format jsonl | grep -q '"groups":1'
	rm -rf $(DIST_DIR)/verify
	@echo "$(PACKAGE).tar.gz verified"

clean:
	$(CARGO) clean
	rm -rf $(DIST_DIR)

help:
	@echo "rdedupe $(VERSION) targets:"
	@echo "  check                  format-check + lint + test (the PR gate)"
	@echo "  format-check lint test resource-tests skill-check"
	@echo "  filesystem-acceptance  real FIDEDUPERANGE tests (TMPDIR on Btrfs/XFS)"
	@echo "  benchmark              cargo bench workloads (see docs/benchmarks.md)"
	@echo "  release static install install-user uninstall uninstall-user"
	@echo "  dist dist-check         release archive (TARGET=x86_64-unknown-linux-musl for static)"
