#!/usr/bin/env python3
"""merge_macros.py - Precise duration matching and Round-Robin Queue Logic"""

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

def get_file_duration_ms(path: Path) -> int:
    """Helper to quickly get the duration of a macro file."""
    events = load_json_events(path)
    if not events: return 0
    try:
        times = [int(e.get("Time", 0)) for e in events]
        return max(times) - min(times)
    except: return 0

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
    def __init__(self, rng, all_files):
        self.rng = rng
        self.all_files = [str(f.resolve()) for f in all_files]
        self.pool = list(self.all_files)
        self.rng.shuffle(self.pool)
        
    def get_files_for_time(self, target_minutes, inter_pause_avg_ms):
        selected = []
        current_ms = 0.0
        target_ms = target_minutes * 60000
        
        while current_ms < target_ms:
            if not self.pool:
                self.pool = list(self.all_files)
                self.rng.shuffle(self.pool)
            
            # Find next in queue not in current selection
            pick = None
            for f in self.pool:
                if f not in selected:
                    pick = f
                    break
            
            # If every file is already in 'selected' but we haven't hit target time,
            # we must repeat files (moving through the queue again).
            if not pick: pick = self.pool[0]
            
            dur = get_file_duration_ms(Path(pick))
            # If a file is broken/empty, skip to avoid infinite loop
            if dur <= 0 and len(self.all_files) > 1:
                if pick in self.pool: self.pool.remove(pick)
                continue

            selected.append(pick)
            if pick in self.pool: self.pool.remove(pick)
            
            # Accumulate duration + estimated pause
            current_ms += dur + inter_pause_avg_ms
            
            # Hard safety break (100 macros) to prevent crashes on extremely tiny files
            if len(selected) > 100: break 
            
        return selected

# ==============================================================================
# MAIN LOGIC
# ==============================================================================

def generate_version_for_folder(rng, v_num, folder, selector, target_min, inter_pause_max):
    # Estimate average pause for the selector's time math
    is_time_sensitive = is_time_sensitive_folder(folder)
    avg_pause_ms = 450 if is_time_sensitive else (inter_pause_max * 1000) / 2
    
    selected_paths = selector.get_files_for_time(target_min, avg_pause_ms)
    if not selected_paths: return None, None, None
    
    # Sort: Always First -> Regular -> Always Last
    selected_paths.sort(key=lambda x: (
        0 if "always first" in Path(x).name.lower() else 
        2 if "always last" in Path(x).name.lower() else 1
    ))

    all_evs = []
    manifest_lines = []
    total_pause_ms = 0
    
    for i, path_str in enumerate(selected_paths):
        p = Path(path_str)
        is_special = SPECIAL_KEYWORD in p.name.lower() or p.name.lower() == SPECIAL_FILENAME
        
        raw_evs, _ = process_macro_file(load_json_events(p))
        if not raw_evs: continue
        
        evs = preserve_click_integrity(raw_evs)
        if not is_special:
            evs = add_mouse_jitter(evs, rng)
            evs = add_reaction_variance(evs, rng)
        
        pause = 0
        if i > 0:
            pause = rng.randint(100, 800) if is_time_sensitive else rng.randint(500, inter_pause_max * 1000)
            total_pause_ms += pause
                
        all_evs = merge_events_with_pauses(all_evs, evs, pause)
        letter = number_to_letters(i+1)
        manifest_lines.append(f"  {letter}: {p.name}")
    
    if not all_evs: return None, None, None
    
    final_min = int(all_evs[-1]['Time'] / 60000)
    afk_min = round(total_pause_ms / 60000, 2)
    v_code = number_to_letters(v_num)
    
    fname = f"{v_code}_{final_min}m.json"
    
    manifest_entry = (
        f"FILENAME: {fname}\n"
        f"TOTAL DURATION: {final_min} minutes\n"
        f"TOTAL AFK TIME: {afk_min} minutes\n"
        f"COMPONENTS:\n" + "\n".join(manifest_lines) + "\n" + "-"*30
    )
    
    return fname, all_evs, manifest_entry

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=Path)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--between-max-time", type=int, default=18)
    parser.add_argument("--target-minutes", type=int, default=25)
    parser.add_argument("--exclude-count", type=int, default=0) 
    args = parser.parse_args()

    rng = random.Random()
    bundle_n = read_counter(COUNTER_PATH)
    bundle_dir = args.output_root / f"merged_bundle_{bundle_n}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Starting merge for bundle {bundle_n}...")
    
    folders = find_all_dirs_with_json(args.input_root)
    print(f"Found {len(folders)} groups with JSON files.")

    for folder in folders:
        files = find_json_files_in_dir(folder)
        if not files: continue
        
        rel_path = folder.relative_to(args.input_root)
        out_folder = bundle_dir / rel_path
        out_folder.mkdir(parents=True, exist_ok=True)
        
        print(f"Processing group: {rel_path}")
        
        selector = QueueFileSelector(rng, files)
        folder_manifest = [f"MANIFEST FOR FOLDER: {rel_path}\n{'='*40}\n"]
        
        for v in range(1, args.versions + 1):
            fname, evs, m_entry = generate_version_for_folder(
                rng, v, folder, selector, 
                args.target_minutes, args.between_max_time
            )
            if fname and evs:
                (out_folder / fname).write_text(json.dumps(evs, indent=2), encoding="utf-8")
                folder_manifest.append(m_entry)
        
        if len(folder_manifest) > 1:
            (out_folder / "manifest.txt").write_text("\n\n".join(folder_manifest), encoding="utf-8")
                
    write_counter(COUNTER_PATH, bundle_n + 1)

if __name__ == "__main__":
    main()
