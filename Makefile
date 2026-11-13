# rdedupe Makefile
# Rust build system with colorized output. Colour is used only when stderr is a
# terminal and NO_COLOR is unset, so CI logs and redirected output stay plain.

# Recipes run under bash with pipefail: a failing command piped into grep or tee fails
# its target. Never pipe into `grep -q` or `head`: they exit early, the writer dies of
# SIGPIPE (141) and the target fails at random; read the whole input instead.
SHELL := bash
.SHELLFLAGS := -o pipefail -c

# ============== Colors & Symbols ==============
COLOR := $(if $(NO_COLOR),,$(shell [ -t 2 ] && echo 1))
ifeq ($(COLOR),1)
COPPER := \033[38;2;217;122;52m
TEAL := \033[38;2;126;226;214m
GREEN := \033[92m
CYAN := \033[96m
YELLOW := \033[93m
RED := \033[91m
GRAY := \033[90m
BOLD := \033[1m
RESET := \033[0m
endif

CHECK := ✓
CROSS := ✗
PROGRESS := →

# ============== Project Metadata ==============
BINARY := rdedupe
VERSION := $(shell awk -F'"' '/^version = /{print $$2; exit}' Cargo.toml)
RUST_VERSION := $(shell awk -F'"' '/^rust-version = /{print $$2; exit}' Cargo.toml)
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

SUBMAKE := $(MAKE) --no-print-directory NO_BANNER=1

# ============== Recipe Helpers ==============
# $(call step,label,command,success,failure): a progress line, then a check or a cross.
# Arguments cannot contain commas; long commands live in the *_CMD variables below.
define step
	@printf "$(PROGRESS) $(1)...\n"
	@$(2) && \
		printf "$(GREEN)$(CHECK) $(3)$(RESET)\n" || \
		(printf "$(RED)$(CROSS) $(4)$(RESET)\n" && exit 1)
endef

# $(call section,title): a boxed heading.
define section
	@printf "$(CYAN)$(BOLD)╔══════════════════════════════════════════╗$(RESET)\n"
	@printf "$(CYAN)$(BOLD)║ %-40s ║$(RESET)\n" "$(1)"
	@printf "$(CYAN)$(BOLD)╚══════════════════════════════════════════╝$(RESET)\n\n"
endef

# Files every release archive must contain.
DIST_FILES := rdedupe LICENSE README.md \
	skills/rdedupe/SKILL.md skills/rdedupe/references/commands.md \
	skills/rdedupe/references/safety.md skills/rdedupe/references/workflows.md \
	skills/rdedupe/references/recovery.md skills/rdedupe/templates/completion-report.md \
	skills/rdedupe/scripts/summarize.py \
	docs/safety.md docs/formats.md docs/cli.md docs/design.md docs/migration.md \
	docs/agent-operation.md docs/benchmarks.md \
	docs/assets/mascot.png docs/assets/scan.png docs/assets/cleanup.png

STAGE := $(DIST_DIR)/$(PACKAGE)
VERIFY := $(DIST_DIR)/verify/$(PACKAGE)
DIST_CMD := rm -rf $(STAGE) $(STAGE).tar.gz $(STAGE).tar.gz.sha256 \
	&& mkdir -p $(STAGE)/skills $(STAGE)/docs \
	&& cp $(RELEASE_DIR)/$(BINARY) LICENSE README.md $(STAGE)/ \
	&& cp -R skills/rdedupe $(STAGE)/skills/ \
	&& cp docs/*.md $(STAGE)/docs/ && cp -R docs/assets $(STAGE)/docs/ \
	&& tar -C $(DIST_DIR) -czf $(STAGE).tar.gz $(PACKAGE) \
	&& (cd $(DIST_DIR) && sha256sum $(PACKAGE).tar.gz > $(PACKAGE).tar.gz.sha256)
CONTENTS_CMD := for f in $(DIST_FILES); do test -f $(VERIFY)/$$f || { echo "missing $$f"; exit 1; }; done
SMOKE_CMD := $(VERIFY)/rdedupe --version | grep -x "rdedupe $(VERSION)" >/dev/null \
	&& grep 'version: "$(VERSION)"' $(VERIFY)/skills/rdedupe/SKILL.md >/dev/null \
	&& mkdir -p $(DIST_DIR)/verify/tree \
	&& printf same > $(DIST_DIR)/verify/tree/a && printf same > $(DIST_DIR)/verify/tree/b \
	&& $(VERIFY)/rdedupe scan $(DIST_DIR)/verify/tree --format jsonl | grep '"groups":1' >/dev/null
BENCH_DIR ?= benchmark-output
COMPARE_CMD := mkdir -p $(BENCH_DIR) && python3 benches/compare.py $(BENCH_DIR)/fixtures \
	$(BENCH_DIR)/compare.json baseline=$(BASELINE) rdedupe-$(VERSION)=$(RELEASE_DIR)/$(BINARY)

# ============== Phony Targets ==============
.PHONY: banner help all build release build-release static static-x86 static-arm \
	install uninstall install-user uninstall-user \
	test resource-tests skill-check filesystem-acceptance \
	lint format fmt format-check check \
	benchmark benchmark-compare screenshots dist dist-check version clean

# ============== Default Target ==============
.DEFAULT_GOAL := help

# ============== Banner ==============
# Drawn once per run: sub-makes get NO_BANNER=1. Without figlet it prints the plain name.
banner:
ifeq ($(NO_BANNER),)
	@{ command -v figlet >/dev/null && figlet -f small rdedupe || echo " rdedupe"; } \
		| { printf "$(COPPER)$(BOLD)"; cat; printf "$(RESET)"; }
	@printf "  $(GRAY)v$(VERSION)$(RESET) $(TEAL)exact, recoverable deduplication$(RESET)\n\n"
endif

# ============== Build Targets ==============

all: banner
	@$(SUBMAKE) check
	@$(SUBMAKE) release

build: banner
	$(call section,Debug Build)
	$(call step,Building rdedupe,$(CARGO) build --locked,Build successful,Build failed)
	@printf "\n"

release: banner
	$(call section,Release Build)
	$(call step,Building the release binary $(TARGET),$(CARGO) build --release --locked $(TARGET_FLAG),Release binary at $(RELEASE_DIR)/$(BINARY),Release build failed)
	@printf "\n"

build-release: release

# Static Linux binaries (musl). Need a musl C compiler for the bundled SQLite.
static: static-x86

static-x86:
	@$(MAKE) --no-print-directory release TARGET=x86_64-unknown-linux-musl

static-arm:
	@$(MAKE) --no-print-directory release TARGET=aarch64-unknown-linux-musl

# ============== Install ==============

install: release
	$(call step,Installing to $(DESTDIR)$(BINDIR),install -d $(DESTDIR)$(BINDIR) && install -m 755 $(RELEASE_DIR)/$(BINARY) $(DESTDIR)$(BINDIR)/$(BINARY),Installed $(BINARY) to $(DESTDIR)$(BINDIR),Install failed)

uninstall:
	$(call step,Removing $(DESTDIR)$(BINDIR)/$(BINARY),rm -f $(DESTDIR)$(BINDIR)/$(BINARY),Uninstalled,Uninstall failed)

install-user: banner
	$(call step,Installing with cargo install,$(CARGO) install --locked --path .,Installed $(BINARY) with cargo,cargo install failed)

uninstall-user:
	$(call step,Removing the cargo-installed binary,{ $(CARGO) uninstall $(BINARY) 2>/dev/null || true; },Uninstalled,Uninstall failed)

# ============== Test Targets ==============

test: banner
	$(call section,Full Test Suite)
	$(call step,Running every test,$(CARGO) test --locked --all-targets --no-fail-fast,All tests passed,Tests failed)
	@printf "\n"

resource-tests: banner
	$(call step,Running the resource-bound tests,$(CARGO) test --locked --test resources -- --nocapture,Resource tests passed,Resource tests failed)

skill-check: banner
	$(call step,Checking the agent skill against the real CLI,$(CARGO) test --locked --test agent_skill --test agent_workflows,Skill matches the CLI,Skill and CLI disagree)

# Real FIDEDUPERANGE acceptance; set TMPDIR to a Btrfs or XFS directory to exercise it.
filesystem-acceptance: banner
	$(call step,Running the extent-sharing tests in $${TMPDIR:-/tmp},$(CARGO) test --locked --test extent_dedupe -- --nocapture,Extent tests passed,Extent tests failed)

# ============== Lint & Format ==============

lint: banner
	$(call step,Running clippy,$(CARGO) clippy --locked --all-targets --all-features -- -D warnings,Clippy passed,Clippy warnings found)

format:
	$(call step,Formatting code,$(CARGO) fmt --all,Code formatted,Formatting failed)

fmt: format

format-check: banner
	$(call step,Checking formatting,$(CARGO) fmt --all -- --check,Formatting valid,Formatting issues found)

# The PR gate.
check: banner
	$(call section,Check: format + lint + test)
	@$(SUBMAKE) format-check
	@$(SUBMAKE) lint
	@$(SUBMAKE) test
	@printf "$(GREEN)$(BOLD)$(CHECK) All checks passed$(RESET)\n\n"

# ============== Benchmarks & Assets ==============

benchmark: banner
	$(call step,Running the pipeline benchmarks,$(CARGO) bench --locked --bench pipeline,Benchmarks finished,Benchmarks failed)

# Before/after CLI comparison on identical fixtures (docs/benchmarks.md):
#   make benchmark-compare BASELINE=/path/to/rdedupe-2.x BENCH_DIR=/path/with/space
benchmark-compare: release
	@test -n "$(BASELINE)" || { printf "$(RED)$(CROSS) set BASELINE=/path/to/baseline/rdedupe$(RESET)\n"; exit 1; }
	$(call step,Comparing against $(BASELINE),$(COMPARE_CMD),Results in $(BENCH_DIR)/compare.json,Comparison failed)

# README screenshots from real runs of the release binary (needs Chromium).
screenshots: release
	$(call step,Rendering docs/assets/scan.png and cleanup.png,python3 scripts/screenshots.py $(RELEASE_DIR)/$(BINARY) docs/assets,Screenshots rendered,Screenshot rendering failed)

# ============== Release Archive ==============

# Binary, license, README, docs and the version-paired agent skill.
dist: release
	$(call section,Release Archive)
	$(call step,Packaging $(STAGE).tar.gz,$(DIST_CMD),Archive and checksum written,Packaging failed)

# Verifies the archive that ships: checksum, contents, and the extracted binary itself.
dist-check: banner
	$(call step,Verifying the checksum,(cd $(DIST_DIR) && sha256sum -c $(PACKAGE).tar.gz.sha256 >/dev/null),Checksum matches,Checksum mismatch)
	@rm -rf $(DIST_DIR)/verify && mkdir -p $(DIST_DIR)/verify && tar -C $(DIST_DIR)/verify -xzf $(STAGE).tar.gz
	$(call step,Checking the archive contents,$(CONTENTS_CMD),Every file is present,Archive is missing files)
	$(call step,Running the extracted binary,$(SMOKE_CMD),Binary and skill are $(VERSION) and find duplicates,Extracted binary failed)
	@rm -rf $(DIST_DIR)/verify
	@printf "$(GREEN)$(BOLD)$(CHECK) $(PACKAGE).tar.gz verified$(RESET)\n"

# ============== Version ==============

version:
	@printf "$(CYAN)Current version:$(RESET) $(YELLOW)$(BOLD)$(VERSION)$(RESET)\n"

# ============== Clean ==============

clean:
	$(call step,Cleaning build artifacts,$(CARGO) clean && rm -rf $(DIST_DIR),Clean complete,Clean failed)

# ============== Help ==============

help: banner
	@printf "$(CYAN)$(BOLD)Build:$(RESET)\n"
	@printf "  $(GREEN)make build$(RESET)                 Debug build\n"
	@printf "  $(GREEN)make release$(RESET)               Release build ($(YELLOW)TARGET=$(RESET)triple to cross-compile)\n"
	@printf "  $(GREEN)make static$(RESET)                Static Linux binary (musl; static-arm for aarch64)\n"
	@printf "  $(GREEN)make install$(RESET)               Install to $(BINDIR) ($(YELLOW)PREFIX=$(RESET) $(YELLOW)DESTDIR=$(RESET))\n"
	@printf "  $(GREEN)make install-user$(RESET)          cargo install from this checkout\n"
	@printf "\n$(CYAN)$(BOLD)Test:$(RESET)\n"
	@printf "  $(GREEN)make check$(RESET)                 $(YELLOW)$(BOLD)The PR gate:$(RESET) format-check, lint, test\n"
	@printf "  $(GREEN)make test$(RESET)                  Every test\n"
	@printf "  $(GREEN)make resource-tests$(RESET)        Memory, descriptor and scratch-space bounds\n"
	@printf "  $(GREEN)make skill-check$(RESET)           Agent skill against the real CLI\n"
	@printf "  $(GREEN)make filesystem-acceptance$(RESET) Extent sharing ($(YELLOW)TMPDIR=$(RESET)a Btrfs or XFS directory)\n"
	@printf "\n$(CYAN)$(BOLD)Lint & Format:$(RESET)\n"
	@printf "  $(GREEN)make lint$(RESET)                  clippy -D warnings\n"
	@printf "  $(GREEN)make format$(RESET)                cargo fmt\n"
	@printf "  $(GREEN)make format-check$(RESET)          Check formatting\n"
	@printf "\n$(CYAN)$(BOLD)Benchmarks & Assets:$(RESET)\n"
	@printf "  $(GREEN)make benchmark$(RESET)             Pipeline workloads (docs/benchmarks.md)\n"
	@printf "  $(GREEN)make benchmark-compare$(RESET)     CLI before/after ($(YELLOW)BASELINE=$(RESET)/path/to/rdedupe)\n"
	@printf "  $(GREEN)make screenshots$(RESET)           Re-render the README screenshots\n"
	@printf "\n$(CYAN)$(BOLD)Release:$(RESET)\n"
	@printf "  $(GREEN)make dist dist-check$(RESET)       Release archive and its verification\n"
	@printf "  $(GREEN)make version$(RESET)               Show the version\n"
	@printf "  $(GREEN)make clean$(RESET)                 Remove build output and archives\n"
	@printf "\n$(GRAY)Rust $(RUST_VERSION)+ · rdedupe v$(VERSION)$(RESET)\n\n"
