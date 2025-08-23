import argparse, json, subprocess, time, uuid, pathlib, sys

def emit(f, **ev):
    f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    f.flush()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cmd", required=True, help='例: "echo hi >/dev/null"')
    ap.add_argument("--runs", type=int, default=50)
    ap.add_argument("--name", default="cmd")
    ap.add_argument("--out", default="results/trace.ndjson")
    ap.add_argument("--sleep-us", type=int, default=0)
    args = ap.parse_args()

    p = pathlib.Path(args.out)
    p.parent.mkdir(parents=True, exist_ok=True)

    with p.open("w", encoding="utf-8") as f:
        for _ in range(args.runs):
            sid = str(uuid.uuid4())
            emit(f, schema="trace.v1", ts_ns=time.perf_counter_ns(),
                 ev="span_start", name=args.name, span_id=sid)
            try:
                subprocess.run(
                    args.cmd, shell=True, check=True,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
                emit(f, schema="trace.v1", ts_ns=time.perf_counter_ns(),
                     ev="span_end", name=args.name, span_id=sid)
            except subprocess.CalledProcessError as e:
                emit(f, schema="trace.v1", ts_ns=time.perf_counter_ns(),
                     ev="span_error", name=args.name, span_id=sid, code=e.returncode)
            if args.sleep_us:
                time.sleep(args.sleep_us / 1_000_000)
    print(f"[measure_cmd] wrote {args.runs} spans -> {args.out}")

if __name__ == "__main__":
    sys.exit(main())
