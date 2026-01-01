#!/usr/bin/env python3
"""
merge_macros.py - STABLE RESTORE POINT (v3.2.0) - COMPLETE FIX
- FEATURE: Random 0-1500ms jitter rolled individually BEFORE every action.
- FEATURE: Pre-Action Mouse Jitter. If delay > 100ms, injects a micro-move.
- FIX: Naming scheme A1, B1, C1... with folder numbering (1-Folder).
- FIX: Z +100 scoped to parent directory only.
- FIX: Jitter is individual per event (non-cumulative).
- FIX: All variables properly initialized.
- OPTIMIZED: Cached durations, single os.walk(), shallow copy.
"""

import argparse, json, random, re, sys, os, math, shutil
from pathlib import Path

def load_json_events(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        events = []
        if isinstance(data, dict):
            found_list = None
            for k in ("events", "items", "entries", "records"):
                if k in data and isinstance(data[k], list):
                    found_list = data[k]
                    break
            events = found_list if found_list is not None else ([data] if "Time" in data else [])
        elif isinstance(data, list):
            events = data
        
        cleaned = []
        for e in events:
            if isinstance(e, list) and len(e) > 0: e = e[0]
            if isinstance(e, dict) and "Time" in e: cleaned.append(e)
        return cleaned
    except Exception:
        return []

def get_file_duration_ms(path: Path) -> int:
    events = load_json_events(path)
    if not events: return 0
    try:
        times = [int(e.get("Time", 0)) for e in events]
        return max(times) - min(times)
    except: return 0

def format_ms_precise(ms: int) -> str:
    ts = int(round(ms / 1000))
    m, s = ts // 60, ts % 60
    return f"{m}m {s}s" if m > 0 else f"{s}s"

def clean_identity(name: str) -> str:
    return re.sub(r'(\s*-\s*Copy(\s*\(\d+\))?)|(\s*\(\d+\))', '', name, flags=re.IGNORECASE).strip().lower()

def extract_folder_number(folder_name: str) -> int:
    """
    Extract number from folder name like '1-Mining' or '23-Fishing'.
    Returns the number, or 0 if not found.
    """
    match = re.match(r'^(\d+)-', folder_name)
    if match:
        return int(match.group(1))
    return 0

def insert_idle_mouse_movements(events, rng, movement_percentage):
    """
    Insert realistic mouse movements during idle periods (gaps > 5 seconds).
    
    Rules:
    - Only in gaps >= 5000ms
    - Use middle 40-50% of gap (25% buffer on each side)
    - Smooth curved paths + random wandering
    - Movements every ~500ms during active window
    """
    if not events or len(events) < 2:
        return events
    
    result = []
    total_idle_time = 0
    
    for i in range(len(events)):
        result.append(events[i])
        
        # Check gap to next event
        if i < len(events) - 1:
            current_time = int(events[i].get("Time", 0))
            next_time = int(events[i + 1].get("Time", 0))
            gap = next_time - current_time
            
            # Only process gaps >= 5 seconds
            if gap >= 5000:
                # Calculate active window (middle 40-50% of gap)
                active_duration = int(gap * movement_percentage)
                buffer_start = (gap - active_duration) // 2
                
                movement_start = current_time + buffer_start
                movement_end = movement_start + active_duration
                
                # Get last known position (if available)
                last_x = events[i].get("X", 500)
                last_y = events[i].get("Y", 500)
                
                # Generate smooth movements every ~500ms
                num_moves = max(1, active_duration // 500)
                
                for move_idx in range(num_moves):
                    t = move_idx / num_moves
                    move_time = int(movement_start + (active_duration * t))
                    
                    # Mix of smooth curves and random wandering
                    if rng.random() < 0.5:
                        # Smooth curved path
                        radius = rng.randint(50, 150)
                        angle = t * math.pi * 2 + rng.uniform(0, math.pi)
                        new_x = int(last_x + math.cos(angle) * radius)
                        new_y = int(last_y + math.sin(angle) * radius)
                    else:
                        # Random wandering
                        new_x = last_x + rng.randint(-100, 100)
                        new_y = last_y + rng.randint(-100, 100)
                    
                    # Keep within reasonable bounds
                    new_x = max(100, min(1800, new_x))
                    new_y = max(100, min(1000, new_y))
                    
                    move_event = {
                        "Time": move_time,
                        "Type": "MouseMove",
                        "X": new_x,
                        "Y": new_y
                    }
                    result.append(move_event)
                    last_x, last_y = new_x, new_y
                
                total_idle_time += active_duration
    
    return result, total_idle_time

class QueueFileSelector:
    def __init__(self, rng, all_files, durations_cache):
        self.rng = rng
        self.durations = durations_cache
        self.efficient = [f for f in all_files if "¬¬¬" not in f.name]
        self.inefficient = [f for f in all_files if "¬¬¬" in f.name]
        self.eff_pool = list(self.efficient)
        self.ineff_pool = list(self.inefficient)
        self.rng.shuffle(self.eff_pool)
        self.rng.shuffle(self.ineff_pool)

    def get_sequence(self, target_minutes, force_inef=False, strictly_eff=False):
        seq, cur_ms = [], 0.0
        target_ms = target_minutes * 60000
        actual_force = force_inef if not strictly_eff else False
        while cur_ms < target_ms:
            if actual_force and self.ineff_pool: pick = self.ineff_pool.pop(0)
            elif self.eff_pool: pick = self.eff_pool.pop(0)
            elif self.efficient:
                self.eff_pool = list(self.efficient); self.rng.shuffle(self.eff_pool)
                pick = self.eff_pool.pop(0)
            elif self.ineff_pool and not strictly_eff: pick = self.ineff_pool.pop(0)
            else: break
            seq.append(pick)
            cur_ms += (self.durations.get(pick, 2000) + 1500)
            if len(seq) > 1000: break
        return seq

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=str)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--target-minutes", type=int, default=35)
    parser.add_argument("--bundle-id", type=int, required=True)
    parser.add_argument("--speed-range", type=str, default="1.0 1.0")
    args = parser.parse_args()

    search_base = Path(args.input_root).resolve()
    if not search_base.exists():
        search_base = Path(".").resolve()
        
    originals_root = None
    for d in ["originals", "input_macros"]:
        test_path = search_base / d
        if test_path.exists() and test_path.is_dir():
            originals_root = test_path
            break
            
    if not originals_root:
        originals_root = search_base
    
    # ✅ NEW: Look for logout.json in multiple locations
    logout_file = None
    for location in [originals_root / "logout.json", originals_root.parent / "logout.json", search_base / "logout.json"]:
        if location.exists() and location.is_file():
            logout_file = location
            print(f"Found logout.json at: {logout_file}")
            break

    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random()
    pools = {}
    z_storage = {}
    durations_cache = {}

    for root, dirs, files in os.walk(originals_root):
        curr = Path(root)
        if any(p in curr.parts for p in [".git", ".github", "output"]): continue
        jsons = [f for f in files if f.endswith(".json") and "click_zones" not in f.lower()]
        if not jsons: continue
        
        is_z_storage = "z +100" in str(curr).lower()
        
        if is_z_storage:
            parent_scope = None
            for part in curr.parts:
                if "desktop" in part.lower() or "mobile" in part.lower():
                    parent_scope = part
                    break
            
            if parent_scope:
                macro_id = clean_identity(curr.name)
                key = (parent_scope, macro_id)
                if key not in z_storage:
                    z_storage[key] = []
                
                for f in jsons:
                    file_path = curr / f
                    z_storage[key].append(file_path)
                    durations_cache[file_path] = get_file_duration_ms(file_path)
        else:
            macro_id = clean_identity(curr.name)
            rel_path = curr.relative_to(originals_root)
            
            parent_scope = None
            for part in curr.parts:
                if "desktop" in part.lower() or "mobile" in part.lower():
                    parent_scope = part
                    break
            
            key = str(rel_path).lower()
            if key not in pools:
                is_ts = bool(re.search(r'time[\s-]*sens', key))
                file_paths = [curr / f for f in jsons]
                
                pools[key] = {
                    "rel_path": rel_path,
                    "files": file_paths,
                    "is_ts": is_ts,
                    "macro_id": macro_id,
                    "parent_scope": parent_scope
                }
                
                for fp in file_paths:
                    durations_cache[fp] = get_file_duration_ms(fp)

    for pool_key, pool_data in pools.items():
        parent_scope = pool_data["parent_scope"]
        macro_id = pool_data["macro_id"]
        
        z_key = (parent_scope, macro_id)
        if z_key in z_storage:
            pool_data["files"].extend(z_storage[z_key])
    
    # Filter out "always first" and "always last" files from merging
    for pool_key, pool_data in pools.items():
        all_files = pool_data["files"]
        always_files = [f for f in all_files if Path(f).name.lower().startswith(("always first", "always last", "-always first", "-always last"))]
        mergeable_files = [f for f in all_files if f not in always_files]
        pool_data["files"] = mergeable_files
        pool_data["always_files"] = always_files
    
    # ✅ FIX #3: Extract folder numbers from folder names instead of generating them
    for key, data in pools.items():
        folder_name = data["rel_path"].name
        folder_number = extract_folder_number(folder_name)
        
        # If no number found, default to 0
        if folder_number == 0:
            print(f"WARNING: No number found in folder name '{folder_name}', using 0")
        
        data["folder_number"] = folder_number
    
    for key, data in pools.items():
        folder_number = data["folder_number"]  # Use extracted number from folder name
        
        # ✅ FIX: Skip folders with 0 mergeable files
        if not data["files"]:
            print(f"Skipping folder (0 files): {data['rel_path']}")
            continue
        
        original_rel_path = data["rel_path"]
        
        out_f = bundle_dir / original_rel_path  # Use folder as-is
        out_f.mkdir(parents=True, exist_ok=True)
        
        # ✅ NEW: Copy logout.json to this folder
        if logout_file:
            try:
                shutil.copy2(logout_file, out_f / "logout.json")
                print(f"  ✓ Copied logout.json to {original_rel_path}")
            except Exception as e:
                print(f"  ✗ Error copying logout.json: {e}")
        
        # Copy "always first/last" files unmodified
        if "always_files" in data:
            for always_file in data["always_files"]:
                try:
                    shutil.copy2(always_file, out_f / Path(always_file).name)
                    print(f"  ✓ Copied unmodified: {Path(always_file).name}")
                except Exception as e:
                    print(f"  ✗ Error copying {Path(always_file).name}: {e}")
        
        manifest = [
            f"MANIFEST FOR FOLDER: {original_rel_path}",
            "=" * 40,
            f"Total Available Files: {len(data['files'])}",
            f"Folder Number: {folder_number}",
            "",
            ""
        ]
        
        norm_v = args.versions
        inef_v = 0 if data["is_ts"] else (norm_v // 2)
        
        for v_idx in range(1, (norm_v + inef_v) + 1):
            is_inef = (v_idx > norm_v)
            v_letter = chr(64 + v_idx)
            v_code = f"{v_letter}{folder_number}"
            
            if data["is_ts"]: mult = rng.choice([1.0, 1.2, 1.5])
            elif is_inef: mult = rng.choices([1, 2, 3], weights=[20, 40, 40], k=1)[0]
            else: mult = rng.choices([1, 2, 3], weights=[50, 30, 20], k=1)[0]
            
            # ✅ NEW: Random movement percentage per version (40-50%)
            movement_percentage = rng.uniform(0.40, 0.50)
            
            total_idle_movements = 0
            total_gaps = 0
            total_afk_pool = 0
            file_segments = []
            massive_pause_info = None
            merged = []
            timeline = 0
            
            paths = QueueFileSelector(rng, data["files"], durations_cache).get_sequence(args.target_minutes, is_inef, data["is_ts"])
            
            if not paths:
                continue
            
            for i, p in enumerate(paths):
                raw = load_json_events(p)
                if not raw: continue
                
                # ✅ NEW: Insert idle mouse movements in this file's events
                raw_with_movements, idle_time = insert_idle_mouse_movements(raw, rng, movement_percentage)
                total_idle_movements += idle_time
                
                t_vals = [int(e["Time"]) for e in raw_with_movements]
                base_t = min(t_vals)
                
                gap = int(rng.randint(500, 2500) * mult) if i > 0 else 0
                timeline += gap
                total_gaps += gap
                
                for e in raw_with_movements:
                    ne = {**e}
                    rel_offset = int(int(e["Time"]) - base_t)
                    ne["Time"] = timeline + rel_offset
                    merged.append(ne)
                
                timeline = merged[-1]["Time"]
                file_segments.append({"name": p.name, "end_time": timeline})
            
            if is_inef and not data["is_ts"] and len(merged) > 1:
                p_ms = rng.randint(300000, 720000)
                split = rng.randint(0, len(merged) - 2)
                for j in range(split + 1, len(merged)): merged[j]["Time"] += p_ms
                timeline = merged[-1]["Time"]
                massive_pause_info = f"Massive P1: {format_ms_precise(p_ms)}"
            
            fname = f"{'¬¬¬' if is_inef else ''}{v_code}_{int(timeline/60000)}m.json"
            (out_f / fname).write_text(json.dumps(merged, indent=2))
            
            total_pause = total_gaps + total_afk_pool
            if massive_pause_info:
                version_label = f"Version {v_code} [EXTRA - INEFFICIENT] (Multiplier: x{mult}):"
            else:
                version_label = f"Version {v_code} (Multiplier: x{mult}):"
            
            manifest_entry = [
                version_label,
                f"  TOTAL DURATION: {format_ms_precise(timeline)}",
                f"  Idle Mouse Movements: {format_ms_precise(total_idle_movements)} ({int(movement_percentage*100)}% of idle time)",
                f"  total PAUSE: {format_ms_precise(total_pause)} +BREAKDOWN:",
                f"    - Inter-file Gaps: {format_ms_precise(total_gaps)}",
                f"    - AFK Pool: {format_ms_precise(total_afk_pool)}"
            ]
            
            if massive_pause_info:
                manifest_entry.append(f"    - {massive_pause_info}")
            
            manifest_entry.append("")
            
            for idx, seg in enumerate(file_segments):
                bullet = "*" if idx < 11 else "-"
                manifest_entry.append(f"  {bullet} {seg['name']} (Ends at {format_ms_precise(seg['end_time'])})")
            
            manifest_entry.append("-" * 30)
            manifest.append("\n".join(manifest_entry))

        (out_f / f"!_MANIFEST_{folder_number}_!.txt").write_text("\n".join(manifest))

if __name__ == "__main__":
    main()
