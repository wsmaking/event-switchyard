.PHONY: bench gate check bless

# 既定値（make 時に上書き可能： make bench RUNS=5 など）
OUT  ?= var/results/pr.json
MODE ?= quick
RUNS ?= 1
CASE ?= match_engine_hot

bench:
	OUT=$(OUT) MODE=$(MODE) RUNS=$(RUNS) CASE=$(CASE) tools/bench/run.sh

gate:
	tools/gate/run.sh $(OUT)

check: bench gate

# 良い結果をベースラインに昇格（BLESS=1 を付けて gate を実行）
bless:
	BLESS=1 tools/gate/run.sh $(OUT)
