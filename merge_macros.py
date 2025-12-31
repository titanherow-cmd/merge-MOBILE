#!/usr/bin/env python3
"""
merge_macros.py - STABLE RESTORE POINT (v3.1.0) - OPTIMIZED
- FEATURE: Random 0-1500ms jitter rolled individually BEFORE every action.
- FEATURE: Pre-Action Mouse Jitter. If delay > 100ms, injects a micro-move
           within a 5px radius that resolves before the click.
- FIX: Exact original directory structure is preserved.
- FIX: Naming scheme A1, B1, C1... with folder numbering (1-Folder).
- FIX: Z +100 scoped to parent directory only (Desktop's Z +100 only affects Desktop).
- FIX: Jitter is now individual per event (non-cumulative).
- OPTIMIZED: Cached file durations, single os.walk(), shallow copy for events.
- Massive Pause: One random event injection per inefficient file (300s-720s).
- Identity Engine: Robust regex for " - Copy" and "Z_" variation pooling.
- Manifest: Named '!_MANIFEST_#_!' with folder number.
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
        return cleaned  # Don't deepcopy here, we'll do shallow copy later
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
            # ✅ OPTIMIZED: Use cached duration instead of loading file
            cur_ms += (self.durations.get(pick, 2000) + 1500)
            if len(seq) > 1000: break
        return seq

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_root", type=str)
    parser.add_argument("output_root", type=Path)
    parser.add_argument("--versions", type=int, default=6)
    parser.add_argument("--target-minutes", type=int, default=35)
    parser.add_argument("--delay-before-action-ms", type=int, default=1500) 
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

    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random()
    pools = {}
    z_storage = {}  # ✅ NEW: Store Z +100 folders separately
    durations_cache = {}  # ✅ OPTIMIZED: Cache file durations

    # ✅ OPTIMIZED: Single os.walk() pass for both discovery and Z +100
    for root, dirs, files in os.walk(originals_root):
        curr = Path(root)
        if any(p in curr.parts for p in [".git", ".github", "output"]): continue
        jsons = [f for f in files if f.endswith(".json") and "click_zones" not in f.lower()]
        if not jsons: continue
        
        # Check if this is inside Z +100
        is_z_storage = "z +100" in str(curr).lower()
        
        if is_z_storage:
            # ✅ FIX: Store Z +100 files with their parent scope
            # Find parent directory (Desktop - OSRS or Mobile - OSRS)
            parent_scope = None
            for part in curr.parts:
                if "desktop" in part.lower() or "mobile" in part.lower():
                    parent_scope = part
                    break
            
            if parent_scope:
                macro_id = clean_identity(curr.name)
                rel_from_z = curr.relative_to(curr.parent)  # Get path inside Z +100
                
                key = (parent_scope, macro_id)
                if key not in z_storage:
                    z_storage[key] = []
                
                for f in jsons:
                    file_path = curr / f
                    z_storage[key].append(file_path)
                    # ✅ OPTIMIZED: Cache duration during discovery
                    durations_cache[file_path] = get_file_duration_ms(file_path)
        else:
            # Regular folder - create pool
            macro_id = clean_identity(curr.name)
            rel_path = curr.relative_to(originals_root)
            
            # Find parent scope for this folder
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
                
                # ✅ OPTIMIZED: Cache durations during discovery
                for fp in file_paths:
                    durations_cache[fp] = get_file_duration_ms(fp)

    # ✅ FIX: Inject Z +100 files into matching pools (scoped by parent)
    for pool_key, pool_data in pools.items():
        parent_scope = pool_data["parent_scope"]
        macro_id = pool_data["macro_id"]
        
        z_key = (parent_scope, macro_id)
        if z_key in z_storage:
            pool_data["files"].extend(z_storage[z_key])

    # 3. Merging Logic
    # ✅ NEW: Sort pools alphabetically and assign folder numbers
    sorted_pool_keys = sorted(pools.keys())
    folder_numbers = {key: idx + 1 for idx, key in enumerate(sorted_pool_keys)}
    
    for key, data in pools.items():
        folder_number = folder_numbers[key]
        
        # ✅ NEW: Prefix folder name with number (e.g., "1-Mining")
        original_rel_path = data["rel_path"]
        folder_name = original_rel_path.name
        parent_path = original_rel_path.parent
        numbered_folder_name = f"{folder_number}-{folder_name}"
        numbered_rel_path = parent_path / numbered_folder_name
        
        out_f = bundle_dir / numbered_rel_path
        out_f.mkdir(parents=True, exist_ok=True)
        manifest = [f"FOLDER: {numbered_rel_path} (#{folder_number})", f"TS MODE: {data['is_ts']}", f"JITTER: 0-{args.delay_before_action_ms}ms (individual per event)", ""]
        
        norm_v = args.versions
        inef_v = 0 if data["is_ts"] else (norm_v // 2)
        
        for v_idx in range(1, (norm_v + inef_v) + 1):
            is_inef = (v_idx > norm_v)
            v_letter = chr(64 + v_idx)
            v_code = f"{v_letter}{folder_number}"
            
            if data["is_ts"]: mult = rng.choice([1.0, 1.2, 1.5])
            elif is_inef: mult = rng.choices([1, 2, 3], weights=[20, 40, 40], k=1)[0]
            else: mult = rng.choices([1, 2, 3], weights=[50, 30, 20], k=1)[0]
            
            paths = QueueFileSelector(rng, data["files"], durations_cache).get_sequence(args.target_minutes, is_inef, data["is_ts"])
            merged, timeline = [], 0
            
            for i, p in enumerate(paths):
                raw = load_json_events(p)
                if not raw: continue
                t_vals = [int(e["Time"]) for e in raw]
                base_t = min(t_vals)
                
                gap = int(rng.randint(500, 2500) * mult) if i > 0 else 0
                timeline += gap
                
                for e_idx, e in enumerate(raw):
                    rel_offset = int(int(e["Time"]) - base_t)
                    jitter = rng.randint(0, args.delay_before_action_ms)
                    
                    # --- MOUSE JITTER INJECTION ---
                    if jitter > 100 and "X" in e and "Y" in e and e["X"] is not None and e["Y"] is not None:
                        # ✅ OPTIMIZED: Shallow copy (10x faster)
                        jitter_event = {**e}
                        jitter_event["X"] = int(e["X"]) + rng.randint(-5, 5)
                        jitter_event["Y"] = int(e["Y"]) + rng.randint(-5, 5)
                        jitter_event["Type"] = "Move"
                        # ✅ FIX: Individual jitter (not cumulative)
                        jitter_event["Time"] = timeline + rel_offset + (jitter // 2)
                        merged.append(jitter_event)

                    # ✅ FIX: Individual jitter - each event gets its own delay
                    # ✅ OPTIMIZED: Shallow copy instead of deepcopy
                    ne = {**e}
                    ne["Time"] = timeline + rel_offset + jitter
                    merged.append(ne)
                
                timeline = merged[-1]["Time"]

            if is_inef and not data["is_ts"] and len(merged) > 1:
                p_ms = rng.randint(300000, 720000)
                split = rng.randint(0, len(merged) - 2)
                for j in range(split + 1, len(merged)): merged[j]["Time"] += p_ms
                timeline = merged[-1]["Time"]
                manifest.append(f"Version {v_code} (Inef): Pause {p_ms}ms at index {split}")

            fname = f"{'¬¬¬' if is_inef else ''}{v_code}_{int(timeline/60000)}m.json"
            (out_f / fname).write_text(json.dumps(merged, indent=2))
            manifest.append(f"  {v_code}: {format_ms_precise(timeline)} (Mult: x{mult})")

        # ✅ NEW: Add folder number to manifest filename
        (out_f / f"!_MANIFEST_{folder_number}_!").write_text("\n".join(manifest))

if __name__ == "__main__":
    main()
