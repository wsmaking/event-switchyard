import csv, os
from collections import defaultdict
import matplotlib.pyplot as plt

src = os.path.join("results", "summary.csv")
rows = []
with open(src, newline="") as f:
    rdr = csv.DictReader(f)
    for r in rdr:
        # 型変換
        r["linger_ms"] = int(r["linger_ms"])
        r["batch_size"] = int(r["batch_size"])
        r["p50_us"] = int(r["p50_us"])
        r["p95_us"] = int(r["p95_us"])
        r["p99_us"] = int(r["p99_us"])
        r["received"] = int(r["received"])
        rows.append(r)

# acks ごとに、linger を横軸、batch ごとに線を分ける
def plot_metric(metric, ylabel):
    os.makedirs("results/plots", exist_ok=True)
    for acks in sorted(set(r["acks"] for r in rows)):
        data = defaultdict(list)  # key=(batch), val=[(linger, value)]
        for r in rows:
            if r["acks"] != acks: continue
            data[r["batch_size"]].append((r["linger_ms"], r[metric]))
        plt.figure()
        for batch, pts in sorted(data.items()):
            pts.sort()
            xs = [x for x,_ in pts]
            ys = [y for _,y in pts]
            plt.plot(xs, ys, marker="o", label=f"batch={batch}")
        plt.title(f"{metric} by linger (acks={acks})")
        plt.xlabel("linger.ms")
        plt.ylabel(ylabel)
        plt.grid(True, which="both", linestyle="--", alpha=0.3)
        plt.legend()
        out = os.path.join("results", "plots", f"{metric}_acks-{acks}.png")
        plt.savefig(out, bbox_inches="tight")
        plt.close()
        print("wrote", out)

plot_metric("p95_us", "p95 (us)")
plot_metric("p99_us", "p99 (us)")
plot_metric("received", "received (msgs/30s)")
print("done.")
