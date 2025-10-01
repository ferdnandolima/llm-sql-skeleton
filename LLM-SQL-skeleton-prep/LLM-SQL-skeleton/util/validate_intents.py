import sys, json
from intents_loader import load_intents

if __name__ == "__main__":
  dir_ = sys.argv[1] if len(sys.argv) > 1 else "config/intents"
  reg = load_intents(dir_)
  print(json.dumps({"count": len(reg), "sample": list(reg.keys())[:10]}, ensure_ascii=False, indent=2))
