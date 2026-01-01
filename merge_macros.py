#!/usr/bin/env python3
"""
merge_macros.py - v3.4.1
- FIX: Improved folder discovery to prevent empty output folders.
- FIX: Resolved TypeError in mouse movement calculation.
- REMOVED: All pre-action jitter/micro-mouse moves.
- FEATURE: Random micro-delay (0-100ms) per event for timing variance.
- FEATURE: Idle Mouse Movements for gaps > 5s.
"""

import argparse, json, random, re, sys, os, math, shutil
from pathlib import Path
from copy import deepcopy

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
    # Strips " - Copy", " (1)", etc.
    cleaned = re.sub(r'(\s*-\s*Copy(\s*\(\d+\))?)|(\s*\(\d+\))', '', name, flags=re.IGNORECASE).strip().lower()
    return cleaned if cleaned else name.lower()

def extract_folder_number(folder_name: str) -> int:
    match = re.match(r'^(\d+)-', folder_name)
    if match: return int(match.group(1))
    return 0

def insert_idle_mouse_movements(events, rng, movement_percentage):
    if not events or len(events) < 2:
        return events, 0
    
    result = []
    total_idle_time = 0
    
    for i in range(len(events)):
        result.append(events[i])
        
        if i < len(events) - 1:
            curr_t = int(events[i].get("Time", 0))
            next_t = int(events[i + 1].get("Time", 0))
            gap = next_t - curr_t
            
            if gap >= 5000:
                active_dur = int(gap * movement_percentage)
                movement_start = curr_t + ((gap - active_dur) // 2)
                
                # Sanitize coordinates
                last_x, last_y = 500, 500 
                for j in range(i, -1, -1):
                    x_v, y_v = events[j].get("X"), events[j].get("Y")
                    if x_v is not None and y_v is not None:
                        try:
                            last_x, last_y = int(x_v), int(y_v)
                            break
                        except: continue
                
                num_moves = max(1, active_dur // 500)
                for m_idx in range(num_moves):
                    t_ratio = m_idx / num_moves
                    move_time = int(movement_start + (active_dur * t_ratio))
                    
                    if rng.random() < 0.5:
                        radius = rng.randint(50, 150)
                        angle = t_ratio * math.pi * 2 + rng.uniform(0, math.pi)
                        new_x = int(last_x + math.cos(angle) * radius)
                        new_y = int(last_y + math.sin(angle) * radius)
                    else:
                        new_x = last_x + rng.randint(-100, 100)
                        new_y = last_y + rng.randint(-100, 100)
                    
                    new_x, new_y = max(100, min(1800, new_x)), max(100, min(1000, new_y))
                    result.append({"Time": move_time, "Type": "MouseMove", "X": new_x, "Y": new_y})
                    last_x, last_y = new_x, new_y
                total_idle_time += active_dur
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
        while cur_ms < target_ms:
            if force_inef and not strictly_eff and self.ineff_pool: pick = self.ineff_pool.pop(0)
            elif self.eff_pool: pick = self.eff_pool.pop(0)
            elif self.efficient:
                self.eff_pool = list(self.efficient); self.rng.shuffle(self.eff_pool)
                pick = self.eff_pool.pop(0)
            elif self.inefficient and not strictly_eff: pick = self.ineff_pool.pop(0)
            else: break
            seq.append(pick)
            cur_ms += (self.durations.get(pick, 2000) + 1500)
            if len(seq) > 2000: break
        return seq

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=str)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--target-minutes", type=int, default=35)
    parser.add_argument("--delay-before-action-ms", type=int, default=100)
    parser.add_argument("--bundle-id", type=int, required=True)
    args = parser.parse_args()

    search_base = Path(args.input_root).resolve()
    # Try to find originals folder, otherwise use search_base
    originals_root = search_base
    for d in ["originals", "input_macros", "macros"]:
        if (search_base / d).exists():
            originals_root = search_base / d
            break
    
    print(f"Scanning source: {originals_root}")

    logout_file = None
    for loc in [search_base / "logout.json", originals_root / "logout.json"]:
        if loc.exists():
            logout_file = loc
            break

    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random()
    pools, z_storage, durations_cache = {}, {}, {}

    # 1. Discovery
    for root, dirs, files in os.walk(originals_root):
        curr = Path(root)
        if any(p in curr.parts for p in [".git", ".github", "output"]): continue
        
        jsons = [f for f in files if f.endswith(".json") and "click_zones" not in f.lower() and f != "logout.json"]
        if not jsons: continue
        
        is_z = "z +" in str(curr).lower() or curr.name.lower().startswith("z_")
        parent_scope = next((p for p in curr.parts if p.lower() in ["desktop", "mobile"]), "default")
        mid = clean_identity(curr.name)

        if is_z:
            z_storage.setdefault((parent_scope, mid), []).extend([curr / f for f in jsons])
            for f in jsons: durations_cache[curr / f] = get_file_duration_ms(curr / f)
        else:
            rel = curr.relative_to(originals_root)
            key = str(rel).lower()
            if key not in pools:
                file_paths = [curr / f for f in jsons]
                pools[key] = {"rel": rel, "files": file_paths, "is_ts": "time-sens" in key or "timesens" in key, "mid": mid, "scope": parent_scope}
                for fp in file_paths: durations_cache[fp] = get_file_duration_ms(fp)

    # 2. Merging Pools
    for k, d in pools.items():
        z_files = z_storage.get((d["scope"], d["mid"]), [])
        d["files"].extend(z_files)
        d["always"] = [f for f in d["files"] if f.name.lower().startswith("always")]
        d["files"] = [f for f in d["files"] if f not in d["always"]]
        d["num"] = extract_folder_number(d["rel"].name)
        print(f"Pool '{k}': Found {len(d['files'])} files.")

    # 3. Execution
    if not pools:
        print("CRITICAL: No macro pools found. Check folder names and JSON locations.")
        return

    for k, data in pools.items():
        out_f = bundle_dir / data["rel"]
        out_f.mkdir(parents=True, exist_ok=True)
        
        if logout_file: shutil.copy2(logout_file, out_f / "logout.json")
        for af in data["always"]: shutil.copy2(af, out_f / af.name)

        norm_v = args.versions
        inef_v = 0 if data["is_ts"] else (norm_v // 2)

        for v_idx in range(1, (norm_v + inef_v) + 1):
            is_inef = (v_idx > norm_v)
            v_code = f"{chr(64 + v_idx)}{data['num']}"
            mult = rng.choice([1.0, 1.2, 1.5]) if data["is_ts"] else (rng.choices([1, 2, 3], weights=[50, 30, 20] if not is_inef else [20, 40, 40])[0])
            
            paths = QueueFileSelector(rng, data["files"], durations_cache).get_sequence(args.target_minutes, is_inef, data["is_ts"])
            if not paths: continue
            
            merged, timeline, total_micro_delay = [], 0, 0
            for i, p in enumerate(paths):
                raw = load_json_events(p)
                if not raw: continue
                raw, _ = insert_idle_mouse_movements(raw, rng, rng.uniform(0.3, 0.4))
                
                base_t = min(int(e["Time"]) for e in raw)
                timeline += (int(rng.randint(500, 2500) * mult) if i > 0 else 0)
                
                for e in raw:
                    rel_off = int(int(e["Time"]) - base_t)
                    micro_delay = rng.randint(0, args.delay_before_action_ms)
                    total_micro_delay += micro_delay
                    
                    ne = deepcopy(e)
                    ne["Time"] = timeline + rel_off + total_micro_delay
                    merged.append(ne)
                timeline = merged[-1]["Time"]

            if is_inef:
                p_ms = rng.randint(300000, 720000)
                split = rng.randint(0, len(merged)-2)
                for j in range(split+1, len(merged)): merged[j]["Time"] += p_ms
                timeline = merged[-1]["Time"]

            fname = f"{'¬¬¬' if is_inef else ''}{v_code}_{int(timeline/60000)}m.json"
            (out_f / fname).write_text(json.dumps(merged, indent=2))

    print(f"Successfully processed {len(pools)} folders.")

if __name__ == "__main__":
    main()
