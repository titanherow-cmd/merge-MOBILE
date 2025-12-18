#!/usr/bin/env python3
"""merge_macros.py - Fixed NameError and Implemented Round-Robin Queue Logic"""

from pathlib import Path
import argparse, json, random, re, sys, os, math, shutil
from copy import deepcopy

COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
SPECIAL_FILENAME = "close reopen mobile screensharelink.json"
SPECIAL_KEYWORD = "screensharelink"

# ==============================================================================
# CORE HELPERS
# ==============================================================================

def read_counter(path: Path) -> int:
    try:
        return int(path.read_text(encoding="utf-8").strip()) if path.exists() else 1
    except: return 1

def write_counter(path: Path, n: int):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(n), encoding="utf-8")

def is_time_sensitive_folder(folder_path: Path) -> bool:
    return "time sensitive" in str(folder_path).lower()

def find_all_dirs_with_json(input_root: Path):
    found = []
    if not input_root.exists(): return found
    for p in sorted(input_root.rglob("*")):
        if p.is_dir():
            if any(child.suffix.lower() == ".json" for child in p.iterdir() if child.is_file()):
                found.append(p)
    return found

def find_json_files_in_dir(dirpath: Path):
    return sorted([p for p in dirpath.glob("*.json") if p.is_file() and "click_zones" not in p.name])

def load_json_events(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            for k in ("events", "items", "entries", "records"):
                if k in data and isinstance(data[k], list): return deepcopy(data[k])
            return [data] if "Time" in data else []
        return deepcopy(data) if isinstance(data, list) else []
    except: return []

def process_macro_file(events: list[dict]) -> tuple[list[dict], int]:
    if not events: return [], 0
    try:
        times = [int(e.get("Time", 0)) for e in events]
        min_t = min(times)
        shifted = []
        for e in events:
            ne = deepcopy(e)
            ne["Time"] = int(e.get("Time", 0)) - min_t
            shifted.append(ne)
        duration = shifted[-1]["Time"] if shifted else 0
        return shifted, duration
    except: return [], 0

def preserve_click_integrity(events):
    preserved = []
    for e in events:
        ne = deepcopy(e)
        # Mark clicks/drags as protected from jitter/reaction variance to prevent breaking logic
        if any(t in str(e.get('Type', '')) for t in ['Down', 'Up', 'Click', 'Button', 'Drag']):
            ne['PROTECTED'] = True
        preserved.append(ne)
    return preserved

def merge_events_with_pauses(base: list[dict], new: list[dict], pause_ms: int) -> list[dict]:
    if not new: return base
    last_t = base[-1]['Time'] if base else 0
    shift = last_t + pause_ms
    shifted = deepcopy(new)
    for e in shifted: e['Time'] = int(e.get('Time', 0)) + shift
    return base + shifted

def number_to_letters(n: int) -> str:
    res = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        res = chr(65 + rem) + res
    return res or "A"

# ==============================================================================
# ANTI-DETECTION
# ==============================================================================

def add_mouse_jitter(events, rng):
    jittered = []
    for e in events:
        ne = deepcopy(e)
        if not e.get('PROTECTED') and 'X' in e and 'Y' in e:
            try:
                ne['X'] = int(e['X']) + rng.randint(-1, 1)
                ne['Y'] = int(e['Y']) + rng.randint(-1, 1)
            except: pass
        jittered.append(ne)
    return jittered

def add_reaction_variance(events, rng):
    varied = []
    offset = 0
    for e in events:
        ne = deepcopy(e)
        if not e.get('PROTECTED') and rng.random() < 0.15:
            offset += rng.randint(-30, 30)
        ne['Time'] = max(0, int(e.get('Time', 0)) + offset)
        varied.append(ne)
    return varied

# ==============================================================================
# ROUND-ROBIN SELECTOR
# ==============================================================================

class QueueFileSelector:
    """Manages files in a queue; used files move to the back of the line."""
    def __init__(self, rng, all_files):
        self.rng = rng
        self.all_files = [str(f.resolve()) for f in all_files]
        self.pool = list(self.all_files)
        self.rng.shuffle(self.pool)
        
    def get_files_for_time(self, target_minutes):
        selected = []
        current_mins = 0.0
        # Average macro length for selection logic
        while current_mins < target_minutes:
            if not self.pool:
                # Refill the pool if all files have been used once
                self.pool = list(self.all_files)
                self.rng.shuffle(self.pool)
            
            # Pick the next file that isn't already in this specific merge version
            pick = None
            for f in self.pool:
                if f not in selected:
                    pick = f
                    break
            
            # If the pool is smaller than the target duration, we must repeat
            if not pick: pick = self.pool[0]
            
            selected.append(pick)
            if pick in self.pool: self.pool.remove(pick)
            current_mins += 2.3 # Estimated min/file
            if len(selected) > 60: break # Safety break
        return selected

# ==============================================================================
# MAIN LOGIC
# ==============================================================================

def generate_version_for_folder(rng, v_num, folder, selector, target_min, inter_pause_max):
    selected_paths = selector.get_files_for_time(target_min)
    if not selected_paths: return None, None
    
    # Sort: "Always First" macros at start, "Always Last" at end
    selected_paths.sort(key=lambda x: (
        0 if "always first" in Path(x).name.lower() else 
        2 if "always last" in Path(x).name.lower() else 1
    ))

    all_evs = []
    parts_info = []
    is_time_sensitive = is_time_sensitive_folder(folder)
    
    for i, path_str in enumerate(selected_paths):
        p = Path(path_str)
        
        # --- FIX: is_special DEFINED HERE TO PREVENT NameError ---
        is_special = SPECIAL_KEYWORD in p.name.lower() or p.name.lower() == SPECIAL_FILENAME
        
        raw_evs, _ = process_macro_file(load_json_events(p))
        if not raw_evs: continue
        
        evs = preserve_click_integrity(raw_evs)
        
        # Apply anti-detection only to non-special macros
        if not is_special:
            evs = add_mouse_jitter(evs, rng)
            evs = add_reaction_variance(evs, rng)
        
        # Handle Inter-file pauses
        pause = 0
        if i > 0:
            if is_time_sensitive:
                pause = rng.randint(100, 800) # Short pauses for time-sensitive
            else:
                pause = rng.randint(500, inter_pause_max * 1000)
                
        all_evs = merge_events_with_pauses(all_evs, evs, pause)
        parts_info.append(f"{number_to_letters(i+1)}[{p.stem}]")
    
    if not all_evs: return None, None
    
    final_min = int(all_evs[-1]['Time'] / 60000)
    v_code = number_to_letters(v_num)
    parts_str = " - ".join(parts_info)
    clean_folder = folder.name.replace(" ", "").replace("-", "")
    fname = f"{clean_folder}_{v_code}_{final_min}m={parts_str}.json"
    
    return fname, all_evs

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=Path)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--between-max-time", type=int, default=18)
    parser.add_argument("--target-minutes", type=int, default=25)
    # Include legacy flag to prevent crashes if workflow isn't fully updated
    parser.add_argument("--exclude-count", type=int, default=0) 
    args = parser.parse_args()

    rng = random.Random()
    bundle_n = read_counter(COUNTER_PATH)
    bundle_dir = args.output_root / f"merged_bundle_{bundle_n}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Starting merge for bundle {bundle_n}...")
    
    folders = find_all_dirs_with_json(args.input_root)
    print(f"Found {len(folders)} macro groups.")

    for folder in folders:
        files = find_json_files_in_dir(folder)
        if not files: continue
        
        print(f"Processing group: {folder.name}")
        out_folder = bundle_dir / folder.name
        out_folder.mkdir(parents=True, exist_ok=True)
        
        # Initialize the Queue Selector for this specific folder
        selector = QueueFileSelector(rng, files)
        
        for v in range(1, args.versions + 1):
            fname, evs = generate_version_for_folder(
                rng, v, folder, selector, 
                args.target_minutes, args.between_max_time
            )
            if fname and evs:
                (out_folder / fname).write_text(json.dumps(evs, indent=2), encoding="utf-8")
                
    # Increment counter for next run
    write_counter(COUNTER_PATH, bundle_n + 1)

if __name__ == "__main__":
    main()
