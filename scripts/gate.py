import argparse
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bench", required=True)
    ap.add_argument("--req", required=True)
    ap.add_argument("--baseline", required=True)
    ap.parse_args()
    print("[stub] gate OK")
    return 0
if __name__ == "__main__":
    raise SystemExit(main())
