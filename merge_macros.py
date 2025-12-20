#!/usr/bin/env python3
"""merge_macros.py - Robust File Discovery with Manifests and AFK Tracking"""

from pathlib import Path
import argparse, json, random, sys, os, math
from copy import deepcopy

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
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}m {seconds}s"

def number_to_letters(n: int) -> str:
    res = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        res = chr(65 + rem) + res
    return res or "A"

class QueueFileSelector:
    """Ensures we use all files in a folder before repeating any."""
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
            
            # Pick next file that isn't already in this specific version if possible
            pick = next((f for f in self.pool if f not in selected), self.pool[0])
            dur = get_file_duration_ms(Path(pick))
            selected.append(pick)
            if pick in self.pool: self.pool.remove(pick)
            
            # Estimate: Duration + gaps
            current_ms += (dur * 1.3) + 1500
            if len(selected) > 150: break 
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

    search_root = args.input_root
    if not search_root.exists():
        if Path("originals").exists():
            search_root = Path("originals")
        else:
            search_root = Path(".")

    rng = random.Random()
    bundle_dir = args.output_root / f"merged_bundle_{args.bundle_id}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"--- DEBUG: SEARCHING FOR MACROS ---")
    
    folders_with_json = []
    # Find folders containing macros
    seen_folders = set()
    for p in search_root.rglob("*.json"):
        if "click_zones" in p.name.lower() or "output" in p.parts or p.name.startswith('.'):
            continue
        if p.parent not in seen_folders:
            folder = p.parent
            jsons = sorted([f for f in folder.glob("*.json") if "click_zones" not in f.name.lower()])
            if jsons:
                folders_with_json.append((folder, jsons))
                seen_folders.add(folder)
                print(f"Found group: {folder.relative_to(search_root)}")

    if not folders_with_json:
        print(f"CRITICAL ERROR: No JSON files found!")
        sys.exit(1)

    for folder_path, json_files in folders_with_json:
        rel_path = folder_path.relative_to(search_root)
        out_folder = bundle_dir / rel_path
        out_folder.mkdir(parents=True, exist_ok=True)
        
        selector = QueueFileSelector(rng, json_files)
        folder_manifest = [f"MANIFEST FOR FOLDER: {rel_path}\n{'='*40}\n"]

        for v in range(1, args.versions + 1):
            selected_paths = selector.get_files_for_time(args.target_minutes)
            if not selected_paths: continue
            
            merged_events = []
            timeline_ms = 0
            accumulated_afk = 0
            total_gaps = 0
            is_time_sensitive = "time sensitive" in str(folder_path).lower()
            
            manifest_entry = [f"Version {number_to_letters(v)}:"]

            for i, p_str in enumerate(selected_paths):
                p = Path(p_str)
                raw = load_json_events(p)
                if not raw: continue
                
                t_vals = [int(e.get("Time", 0)) for e in raw]
                base_t = min(t_vals) if t_vals else 0
                dur = (max(t_vals) - base_t) if t_vals else 0
                
                # Inter-file gap
                gap = rng.randint(500, 2500) if i > 0 else 0
                timeline_ms += gap
                total_gaps += gap
                
                # Calculate AFK contribution
                if "screensharelink" not in p.name.lower():
                    # Random human "thinking" time pool
                    pct = rng.choice([0, 0, 0, 0.12, 0.20, 0.28])
                    accumulated_afk += int(dur * pct)

                for e in raw:
                    ne = deepcopy(e)
                    ne["Time"] = (int(e.get("Time", 0)) - base_t) + timeline_ms
                    merged_events.append(ne)
                
                timeline_ms = merged_events[-1]["Time"]
                manifest_entry.append(f"  - {p.name} (Ends at {format_ms_precise(timeline_ms)})")

            # Apply AFK Pool
            if accumulated_afk > 0:
                if is_time_sensitive:
                    merged_events[-1]["Time"] += accumulated_afk
                else:
                    split_idx = rng.randint(1, len(merged_events) - 1)
                    for k in range(split_idx, len(merged_events)):
                        merged_events[k]["Time"] += accumulated_afk

            final_dur = merged_events[-1]["Time"]
            v_code = number_to_letters(v)
            fname = f"{v_code}_{int(final_dur / 60000)}m.json"
            
            # Save Macro
            (out_folder / fname).write_text(json.dumps(merged_events, indent=2))
            
            # Add to folder manifest
            manifest_entry.append(f"  TOTAL DURATION: {format_ms_precise(final_dur)}")
            manifest_entry.append(f"  HUMANIZATION ADDED (AFK/Gaps): {format_ms_precise(accumulated_afk + total_gaps)}")
            manifest_entry.append("-" * 20)
            folder_manifest.append("\n".join(manifest_entry))

        # Save manifest for this group
        (out_folder / "manifest.txt").write_text("\n\n".join(folder_manifest))
        print(f"Processed folder: {rel_path}")

    print(f"--- SUCCESS: Bundle {args.bundle_id} created ---")

if __name__ == "__main__":
    main()
