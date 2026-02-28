.PHONY: help rust-check rust-test rust-run v3-build-release v3-local-gate v3-stable-gate v3-capability-gate v3-absolute-gate v3-absolute-gate-loop v3-phase2-compare v3-phase2-compare-quiet v3-open-loop v3-capacity-sweep preset-switch-gate bench gate check check-lite check-full bless perf-gate-rust perf-gate-rust-ci perf-gate-rust-nightly perf-gate-rust-bless

.DEFAULT_GOAL := help

RUNS ?= 5
WARMUP_RUNS ?= 1
TARGET_RPS ?= 10000
DURATION ?= 300

help:
	@echo "========================================="
	@echo " Event Switchyard v3 Ops Targets"
	@echo "========================================="
	@echo "  make rust-check            - gateway-rust cargo check"
	@echo "  make rust-test             - gateway-rust cargo test"
	@echo "  make rust-run              - gateway-rust run --release"
	@echo "  make v3-build-release      - build gateway-rust release binary"
	@echo "  make v3-local-gate         - local strict gate (non-Linux fallback)"
	@echo "  make v3-stable-gate        - stable gate (10k/300s)"
	@echo "  make v3-capability-gate    - capability gate (~45k, accepted>=0.95)"
	@echo "  make v3-absolute-gate      - absolute gate (Linux)"
	@echo "  make v3-absolute-gate-loop - absolute gate loop"
	@echo "  make v3-phase2-compare     - baseline/injected compare"
	@echo "  make v3-phase2-compare-quiet - compare after noise wait"
	@echo "  make v3-open-loop          - open-loop probe"
	@echo "  make v3-capacity-sweep     - capacity sweep"
	@echo ""
	@echo "Compatibility aliases:"
	@echo "  make check / check-lite / check-full"
	@echo "  make perf-gate-rust* / bench / gate / bless"
	@echo "========================================="

rust-check:
	@cd gateway-rust && cargo check

rust-test:
	@cd gateway-rust && cargo test

rust-run:
	@cd gateway-rust && cargo run --release

v3-build-release:
	@scripts/ops/build_gateway_rust_release.sh

v3-local-gate:
	@scripts/ops/check_v3_local_strict_gate.sh

v3-stable-gate:
	@scripts/ops/check_v3_stable_gate.sh

v3-capability-gate:
	@scripts/ops/check_v3_capability_gate.sh

v3-absolute-gate:
	@scripts/ops/check_v3_absolute_gate.sh

v3-absolute-gate-loop:
	@scripts/ops/run_v3_absolute_gate_loop.sh

v3-phase2-compare:
	@RUNS=$(RUNS) WARMUP_RUNS=$(WARMUP_RUNS) scripts/ops/run_v3_phase2_compare.sh

v3-phase2-compare-quiet:
	@RUNS=$(RUNS) WARMUP_RUNS=$(WARMUP_RUNS) scripts/ops/run_v3_phase2_compare_when_quiet.sh

v3-open-loop:
	@TARGET_RPS=$(TARGET_RPS) DURATION=$(DURATION) scripts/ops/run_v3_open_loop_probe.sh

v3-capacity-sweep:
	@scripts/ops/run_v3_capacity_sweep.sh

# Compatibility aliases for existing workflows
bench: v3-phase2-compare
gate: v3-local-gate
check: rust-check rust-test
check-lite: rust-check rust-test
check-full: rust-check rust-test v3-local-gate
bless: v3-phase2-compare
perf-gate-rust: v3-phase2-compare
perf-gate-rust-ci: v3-phase2-compare
perf-gate-rust-nightly: v3-phase2-compare
perf-gate-rust-bless: v3-phase2-compare
preset-switch-gate: v3-local-gate
