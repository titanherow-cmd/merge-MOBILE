#!/usr/bin/env python3
"""merge_macros.py - Unified Humanization Engine with Accumulated Single-Block AFK and Time-Sensitive logic"""

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
    events = load_json_events(path)
    if not events: return 0
    try:
        times = [int(e.get("Time", 0)) for e in events]
        return max(times) - min(times)
    except: return 0

def format_ms_precise(ms: int) -> str:
    total_seconds = int(ms / 1000)
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}.Min {seconds}.Sec"

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

def number_to_letters(n: int) -> str:
    res = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        res = chr(65 + rem) + res
    return res or "A"

# ==============================================================================
# UPDATED HUMANIZATION RULES
# ==============================================================================

def apply_micro_hesitation(events, delay_ms, rng):
    """Rule 1: Delay Before Action (40% chance per file)"""
    if delay_ms <= 0 or not events: return events, 0
    if rng.random() > 0.40: return events, 0
    
    modified_events = deepcopy(events)
    total_added = 0
    for i in range(len(modified_events)):
        jitter = rng.randint(-118, 119)
        actual_chunk = delay_ms + jitter
        if actual_chunk < 0: actual_chunk = 0
        for j in range(i, len(modified_events)):
            modified_events[j]['Time'] += actual_chunk
        total_added += actual_chunk
    return modified_events, total_added

def roll_macro_afk_pool(events, rng):
    """Rule 2 Part A: Roll odds per file to add to a global pool"""
    if not events: return 0
    choices = [0, 12, 20, 28]
    weights = [55, 20, 15, 10]
    pct = rng.choices(choices, weights=weights, k=1)[0]
    if pct == 0: return 0
    
    duration = events[-1]['Time'] - events[0]['Time']
    return int(duration * (pct / 100))

def inject_single_afk_block(events, total_pool_ms, rng, is_time_sensitive):
    """Rule 2 Part B: Insert total pool as one massive chunk.
    If time-sensitive, always at the end. Otherwise, random.
    """
    if total_pool_ms <= 0 or not events: return events
    
    modified_events = deepcopy(events)
    if is_time_sensitive:
        # Just return the events; the "Time" of the end is naturally extended 
        # but to make it explicit for logic that might check last event time:
        # We don't actually shift any events if it's at the end, 
        # but we need to ensure the final total duration reflects this.
        # However, since we define duration by the last event's time, 
        # we'll add a dummy "Wait" event at the end or just handle it in duration calc.
        # To keep it simple and compatible, we'll shift a 'virtual' end.
        # Actually, the most robust way is to add the pause to the last event
        # if the last event is a wait, or just let the manifest report it.
        # BUT for the JSON to actually 'last' longer, the last event time must be high.
        modified_events[-1]['Time'] += total_pool_ms
    else:
        idx = rng.randint(1, len(modified_events) - 1)
        for i in range(idx, len(modified_events)):
            modified_events[i]['Time'] += total_pool_ms
    return modified_events

# ==============================================================================
# ROUND-ROBIN SELECTOR
# ==============================================================================

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
            pick = None
            for f in self.pool:
                if f not in selected:
                    pick = f
                    break
            if not pick: pick = self.pool[0]
            dur = get_file_duration_ms(Path(pick))
            if dur <= 0 and len(self.all_files) > 1:
                if pick in self.pool: self.pool.remove(pick)
                continue
            selected.append(pick)
            if pick in self.pool: self.pool.remove(pick)
            current_ms += (dur * 1.5) + 1200
            if len(selected) > 100: break 
        return selected

# ==============================================================================
# MAIN LOGIC
# ==============================================================================

def generate_version_for_folder(rng, v_num, folder, selector, target_min, delay_before_ms):
    is_time_sensitive = "time sensitive" in str(folder).lower()
    selected_paths = selector.get_files_for_time(target_min)
    if not selected_paths: return None, None, None
    
    selected_paths.sort(key=lambda x: (
        0 if "always first" in Path(x).name.lower() else 
        2 if "always last" in Path(x).name.lower() else 1
    ))

    merged_events = []
    manifest_lines = []
    accumulated_afk_pool = 0
    total_inter_file_gap_ms = 0
    total_micro_delay_ms = 0
    
    for i, path_str in enumerate(selected_paths):
        p = Path(path_str)
        is_special = SPECIAL_KEYWORD in p.name.lower() or p.name.lower() == SPECIAL_FILENAME
        
        raw_evs, _ = process_macro_file(load_json_events(p))
        if not raw_evs: continue
        
        evs = preserve_click_integrity(raw_evs)
        
        if not is_special:
            # Rule 1: Micro Hesitations (40% chance)
            evs, micro_ms = apply_micro_hesitation(evs, delay_before_ms, rng)
            total_micro_delay_ms += micro_ms
            
            # Rule 2: Roll for the Pool (Macro AFK)
            added_to_pool = roll_macro_afk_pool(evs, rng)
            accumulated_afk_pool += added_to_pool
        
        # Rule 3: Inter-File Gap (0.5s to 2s, precision ms variety)
        inter_pause = 0
        if i > 0:
            inter_pause = rng.randint(500, 2000)
            total_inter_file_gap_ms += inter_pause
                
        merged_events = merge_events_with_pauses(merged_events, evs, inter_pause)
        
        letter = number_to_letters(i+1)
        manifest_lines.append(f"  {letter}: {p.name} | Total: {format_ms_precise(merged_events[-1]['Time'])}")

    # Rule 2 FINAL: Inject the accumulated pool
    if accumulated_afk_pool > 0:
        merged_events = inject_single_afk_block(merged_events, accumulated_afk_pool, rng, is_time_sensitive)

    total_ms = merged_events[-1]['Time'] if merged_events else 0
    total_combined_afk = accumulated_afk_pool + total_inter_file_gap_ms + total_micro_delay_ms
    
    v_code = number_to_letters(v_num)
    final_min_only = int(total_ms / 60000)
    fname = f"{v_code}_{final_min_only}m.json"
    
    manifest_entry = (
        f"FILENAME: {fname}\n"
        f"TOTAL DURATION: {format_ms_precise(total_ms)}\n"
        f"TOTAL COMBINED AFK: {format_ms_precise(total_combined_afk)}\n"
        f"COMPONENTS:\n" + "\n".join(manifest_lines) + "\n" + "-"*40
    )
    
    return fname, merged_events, manifest_entry

def merge_events_with_pauses(base: list[dict], new: list[dict], pause_ms: int) -> list[dict]:
    if not new: return base
    last_t = base[-1]['Time'] if base else 0
    shift = last_t + pause_ms
    shifted = deepcopy(new)
    for e in shifted: e['Time'] = int(e.get('Time', 0)) + shift
    return base + shifted

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=Path)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--target-minutes", type=int, default=25)
    parser.add_argument("--delay-before-action-ms", type=int, default=10)
    args = parser.parse_args()

    rng = random.Random()
    bundle_n = read_counter(COUNTER_PATH)
    bundle_dir = args.output_root / f"merged_bundle_{bundle_n}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    folders = find_all_dirs_with_json(args.input_root)
    for folder in folders:
        files = find_json_files_in_dir(folder)
        if not files: continue
        
        rel_path = folder.relative_to(args.input_root)
        out_folder = bundle_dir / rel_path
        out_folder.mkdir(parents=True, exist_ok=True)
        
        selector = QueueFileSelector(rng, files)
        folder_manifest = [f"MANIFEST FOR FOLDER: {rel_path}\n{'='*40}\n"]
        
        for v in range(1, args.versions + 1):
            fname, evs, m_entry = generate_version_for_folder(
                rng, v, folder, selector, args.target_minutes, args.delay_before_action_ms
            )
            if fname and evs:
                (out_folder / fname).write_text(json.dumps(evs, indent=2), encoding="utf-8")
                folder_manifest.append(m_entry)
        
        if len(folder_manifest) > 1:
            (out_folder / "manifest.txt").write_text("\n\n".join(folder_manifest), encoding="utf-8")
                
    write_counter(COUNTER_PATH, bundle_n + 1)

if __name__ == "__main__":
    main()
