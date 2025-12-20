#!/usr/bin/env python3
"""merge_macros.py - Unified Humanization Engine with explicit output paths"""

from pathlib import Path
import argparse, json, random, sys, os, math
from copy import deepcopy

COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
SPECIAL_FILENAME = "close reopen mobile screensharelink.json"
SPECIAL_KEYWORD = "screensharelink"

def load_json_events(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            for k in ("events", "items", "entries", "records"):
                if k in data and isinstance(data[k], list): return deepcopy(data[k])
            return [data] if "Time" in data else []
        return deepcopy(data) if isinstance(data, list) else []
    except Exception as e:
        print(f"Error loading {path.name}: {e}")
        return []

def get_file_duration_ms(path: Path) -> int:
    events = load_json_events(path)
    if not events: return 0
    try:
        times = [int(e.get("Time", 0)) for e in events]
        return max(times) - min(times)
    except: return 0

def format_ms_precise(ms: int) -> str:
    total_seconds = int(ms / 1000)
    return f"{total_seconds // 60}.Min {total_seconds % 60}.Sec"

def number_to_letters(n: int) -> str:
    res = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        res = chr(65 + rem) + res
    return res or "A"

class QueueFileSelector:
    def __init__(self, rng, all_files):
        self.rng = rng
        self.all_files = [str(f.resolve()) for f in all_files]
        self.pool = list(self.all_files)
        self.rng.shuffle(self.pool)
        
    def get_files_for_time(self, target_minutes):
        selected = []
        current_ms = 0.0
        target_ms = target_minutes * 60000
        while current_ms < target_ms:
            if not self.pool:
                self.pool = list(self.all_files)
                self.rng.shuffle(self.pool)
            pick = next((f for f in self.pool if f not in selected), self.pool[0])
            dur = get_file_duration_ms(Path(pick))
            selected.append(pick)
            if pick in self.pool: self.pool.remove(pick)
            current_ms += (dur * 1.5) + 1200
            if len(selected) > 50: break 
        return selected

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
    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Starting merge into: {bundle_dir}")

    # Find directories containing JSONs
    folders = []
    for p in sorted(args.input_root.rglob("*")):
        if p.is_dir() and any(f.suffix.lower() == ".json" for f in p.iterdir()):
            folders.append(p)

    if not folders:
        print(f"No folders with JSON files found in {args.input_root}")
        return

    for folder in folders:
        json_files = sorted([f for f in folder.glob("*.json") if "click_zones" not in f.name])
        if not json_files: continue
        
        rel_path = folder.relative_to(args.input_root)
        out_folder = bundle_dir / rel_path
        out_folder.mkdir(parents=True, exist_ok=True)
        
        selector = QueueFileSelector(rng, json_files)
        manifest = [f"Folder: {rel_path}\n" + "="*20]

        for v in range(1, args.versions + 1):
            selected = selector.get_files_for_time(args.target_minutes)
            if not selected: continue
            
            merged_events = []
            current_time = 0
            is_time_sensitive = "time sensitive" in str(folder).lower()
            pool_afk = 0
            
            for i, p_str in enumerate(selected):
                p = Path(p_str)
                raw = load_json_events(p)
                if not raw: continue
                
                # Shift events to start at 0
                t_offsets = [int(e.get("Time", 0)) for e in raw]
                min_t = min(t_offsets)
                
                # Inter-file pause
                pause = rng.randint(500, 2000) if i > 0 else 0
                current_time += pause
                
                # Rule: Accumulate AFK pool (Simplified for logic check)
                if "screensharelink" not in p.name.lower():
                    dur = max(t_offsets) - min_t
                    pool_afk += int(dur * rng.choice([0, 0.12, 0.20]))

                # Add events
                for e in raw:
                    ne = deepcopy(e)
                    ne["Time"] = (int(e.get("Time", 0)) - min_t) + current_time
                    merged_events.append(ne)
                
                current_time = merged_events[-1]["Time"]
            
            # Time Sensitive Exception: Add pool at the end
            if is_time_sensitive:
                merged_events[-1]["Time"] += pool_afk
            else:
                # Random insert (Simplified: just middle for now to ensure working)
                mid = len(merged_events) // 2
                for k in range(mid, len(merged_events)):
                    merged_events[k]["Time"] += pool_afk

            final_total = merged_events[-1]["Time"]
            v_code = number_to_letters(v)
            fname = f"{v_code}_{int(final_total / 60000)}m.json"
            (out_folder / fname).write_text(json.dumps(merged_events, indent=2))
            manifest.append(f"{v_code}: {fname} ({format_ms_precise(final_total)})")

        (out_folder / "manifest.txt").write_text("\n".join(manifest))
        print(f"Processed folder: {rel_path}")

if __name__ == "__main__":
    main()
