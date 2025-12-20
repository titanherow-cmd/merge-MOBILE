#!/usr/bin/env python3
"""merge_macros.py - Restored Stable Version with Time-Sensitive logic"""

from pathlib import Path
import argparse, json, random, sys, os, math
from copy import deepcopy

COUNTER_PATH = Path(".github/merge_bundle_counter.txt")

def load_json_events(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            for k in ("events", "items", "entries", "records"):
                if k in data and isinstance(data[k], list): return deepcopy(data[k])
            return [data] if "Time" in data else []
        return deepcopy(data) if isinstance(data, list) else []
    except: return []

def get_file_duration_ms(path: Path) -> int:
    events = load_json_events(path)
    if not events: return 0
    try:
        times = [int(e.get("Time", 0)) for e in events]
        return max(times) - min(times)
    except: return 0

def number_to_letters(n: int) -> str:
    res = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        res = chr(65 + rem) + res
    return res or "A"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=Path)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--target-minutes", type=int, default=25)
    parser.add_argument("--delay-before-action-ms", type=int, default=10)
    parser.add_argument("--bundle-id", type=int, required=True)
    args, unknown = parser.parse_known_args()

    rng = random.Random()
    # Path logic strictly matching the YAML
    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Scan for folders containing .json files
    folders = []
    for p in sorted(args.input_root.rglob("*")):
        if p.is_dir() and any(f.suffix.lower() == ".json" for f in p.iterdir() if f.is_file()):
            folders.append(p)

    if not folders:
        print(f"Error: No macro folders found in {args.input_root}")
        return

    for folder in folders:
        json_files = sorted([f for f in folder.glob("*.json") if "click_zones" not in f.name])
        if not json_files: continue
        
        rel_path = folder.relative_to(args.input_root)
        out_folder = bundle_dir / rel_path
        out_folder.mkdir(parents=True, exist_ok=True)
        
        for v in range(1, args.versions + 1):
            # Selection logic
            selected = []
            current_ms = 0.0
            target_ms = args.target_minutes * 60000
            
            # Simple shuffle selection
            pool = list(json_files)
            rng.shuffle(pool)
            while current_ms < target_ms and pool:
                pick = pool.pop(0)
                selected.append(pick)
                current_ms += (get_file_duration_ms(pick) * 1.2) + 1000
            
            if not selected: continue
            
            merged_events = []
            timeline_ms = 0
            accumulated_afk = 0
            is_time_sensitive = "time sensitive" in str(folder).lower()

            for i, p in enumerate(selected):
                raw = load_json_events(p)
                if not raw: continue
                
                # Normalize Time to 0
                t_vals = [int(e.get("Time", 0)) for e in raw]
                base_t = min(t_vals)
                dur = max(t_vals) - base_t
                
                # Gap between files
                gap = rng.randint(500, 2000) if i > 0 else 0
                timeline_ms += gap
                
                # Roll for Human AFK Pool
                if "screensharelink" not in p.name.lower():
                    pct = rng.choice([0, 0.12, 0.20, 0.28])
                    accumulated_afk += int(dur * pct)

                for e in raw:
                    ne = deepcopy(e)
                    ne["Time"] = (int(e.get("Time", 0)) - base_t) + timeline_ms
                    merged_events.append(ne)
                
                timeline_ms = merged_events[-1]["Time"]

            # APPLY AFK POOL
            if accumulated_afk > 0:
                if is_time_sensitive:
                    # Logic: Always at the end
                    merged_events[-1]["Time"] += accumulated_afk
                else:
                    # Logic: Random spot in the middle
                    split_idx = rng.randint(1, len(merged_events) - 1)
                    for k in range(split_idx, len(merged_events)):
                        merged_events[k]["Time"] += accumulated_afk

            final_dur = merged_events[-1]["Time"]
            v_code = number_to_letters(v)
            fname = f"{v_code}_{int(final_dur / 60000)}m.json"
            (out_folder / fname).write_text(json.dumps(merged_events, indent=2))

        print(f"Done: {rel_path}")

if __name__ == "__main__":
    main()#!/usr/bin/env python3
"""merge_macros.py - Restored Stable Version with Time-Sensitive logic"""

from pathlib import Path
import argparse, json, random, sys, os, math
from copy import deepcopy

COUNTER_PATH = Path(".github/merge_bundle_counter.txt")

def load_json_events(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            for k in ("events", "items", "entries", "records"):
                if k in data and isinstance(data[k], list): return deepcopy(data[k])
            return [data] if "Time" in data else []
        return deepcopy(data) if isinstance(data, list) else []
    except: return []

def get_file_duration_ms(path: Path) -> int:
    events = load_json_events(path)
    if not events: return 0
    try:
        times = [int(e.get("Time", 0)) for e in events]
        return max(times) - min(times)
    except: return 0

def number_to_letters(n: int) -> str:
    res = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        res = chr(65 + rem) + res
    return res or "A"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=Path)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--target-minutes", type=int, default=25)
    parser.add_argument("--delay-before-action-ms", type=int, default=10)
    parser.add_argument("--bundle-id", type=int, required=True)
    args, unknown = parser.parse_known_args()

    rng = random.Random()
    # Path logic strictly matching the YAML
    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Scan for folders containing .json files
    folders = []
    for p in sorted(args.input_root.rglob("*")):
        if p.is_dir() and any(f.suffix.lower() == ".json" for f in p.iterdir() if f.is_file()):
            folders.append(p)

    if not folders:
        print(f"Error: No macro folders found in {args.input_root}")
        return

    for folder in folders:
        json_files = sorted([f for f in folder.glob("*.json") if "click_zones" not in f.name])
        if not json_files: continue
        
        rel_path = folder.relative_to(args.input_root)
        out_folder = bundle_dir / rel_path
        out_folder.mkdir(parents=True, exist_ok=True)
        
        for v in range(1, args.versions + 1):
            # Selection logic
            selected = []
            current_ms = 0.0
            target_ms = args.target_minutes * 60000
            
            # Simple shuffle selection
            pool = list(json_files)
            rng.shuffle(pool)
            while current_ms < target_ms and pool:
                pick = pool.pop(0)
                selected.append(pick)
                current_ms += (get_file_duration_ms(pick) * 1.2) + 1000
            
            if not selected: continue
            
            merged_events = []
            timeline_ms = 0
            accumulated_afk = 0
            is_time_sensitive = "time sensitive" in str(folder).lower()

            for i, p in enumerate(selected):
                raw = load_json_events(p)
                if not raw: continue
                
                # Normalize Time to 0
                t_vals = [int(e.get("Time", 0)) for e in raw]
                base_t = min(t_vals)
                dur = max(t_vals) - base_t
                
                # Gap between files
                gap = rng.randint(500, 2000) if i > 0 else 0
                timeline_ms += gap
                
                # Roll for Human AFK Pool
                if "screensharelink" not in p.name.lower():
                    pct = rng.choice([0, 0.12, 0.20, 0.28])
                    accumulated_afk += int(dur * pct)

                for e in raw:
                    ne = deepcopy(e)
                    ne["Time"] = (int(e.get("Time", 0)) - base_t) + timeline_ms
                    merged_events.append(ne)
                
                timeline_ms = merged_events[-1]["Time"]

            # APPLY AFK POOL
            if accumulated_afk > 0:
                if is_time_sensitive:
                    # Logic: Always at the end
                    merged_events[-1]["Time"] += accumulated_afk
                else:
                    # Logic: Random spot in the middle
                    split_idx = rng.randint(1, len(merged_events) - 1)
                    for k in range(split_idx, len(merged_events)):
                        merged_events[k]["Time"] += accumulated_afk

            final_dur = merged_events[-1]["Time"]
            v_code = number_to_letters(v)
            fname = f"{v_code}_{int(final_dur / 60000)}m.json"
            (out_folder / fname).write_text(json.dumps(merged_events, indent=2))

        print(f"Done: {rel_path}")

if __name__ == "__main__":
    main()
