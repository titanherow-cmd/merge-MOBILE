#!/usr/bin/env python3
"""merge_macros.py - OSRS Anti-Detection with AFK & Zone Awareness (Fixed Arguments)"""

from pathlib import Path
import argparse, json, random, re, sys, os, math, shutil
from copy import deepcopy
from zipfile import ZipFile
from itertools import combinations, permutations

COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
SPECIAL_FILENAME = "close reopen mobile screensharelink.json"
SPECIAL_KEYWORD = "screensharelink"

# ==============================================================================
# CORE HELPERS
# ==============================================================================

def parse_time_to_seconds(s: str) -> int:
    if s is None or not str(s).strip():
        raise ValueError("Empty time string")
    s = str(s).strip()
    if re.match(r'^\d+$', s):
        return int(s)
    m = re.match(r'^(\d+):(\d{1,2})$', s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m = re.match(r'^(\d+)\.(\d{1,2})$', s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2).ljust(2, '0')) 
    m = re.match(r'^(?:(\d+)m)?(?:(\d+)s)?$', s)
    if m and (m.group(1) or m.group(2)):
        minutes = int(m.group(1)) if m.group(1) else 0
        seconds = int(m.group(2)) if m.group(2) else 0
        return minutes * 60 + seconds
    raise ValueError(f"Cannot parse time: {s!r}")

def read_counter(path: Path) -> int:
    try:
        if path.exists():
            txt = path.read_text(encoding="utf-8").strip()
            return int(txt) if txt else 1
        return 1
    except Exception:
        return 1

def write_counter(path: Path, n: int):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(n), encoding="utf-8")
    except Exception as e:
        print(f"Error writing bundle counter to {path}: {e}", file=sys.stderr)

def load_exemption_config():
    config_file = Path.cwd() / "exemption_config.json"
    if config_file.exists():
        try:
            data = json.loads(config_file.read_text(encoding="utf-8"))
            return {
                "auto_detect_time_sensitive": data.get("auto_detect_time_sensitive", True),
                "disable_intra_pauses": data.get("disable_intra_pauses", False),
                "disable_inter_pauses": data.get("disable_inter_pauses", False)
            }
        except Exception as e:
            print(f"WARNING: Failed to load exemptions: {e}", file=sys.stderr)
    return {"auto_detect_time_sensitive": True, "disable_intra_pauses": False, "disable_inter_pauses": False}

def is_time_sensitive_folder(folder_path: Path) -> bool:
    folder_str = str(folder_path).lower()
    return "time sensitive" in folder_str

def load_click_zones(folder_path: Path):
    search_paths = [folder_path / "click_zones.json", folder_path.parent / "click_zones.json", Path.cwd() / "click_zones.json"]
    for zone_file in search_paths:
        if zone_file.exists():
            try:
                data = json.loads(zone_file.read_text(encoding="utf-8"))
                return data.get("target_zones", []), data.get("excluded_zones", [])
            except Exception as e:
                print(f"WARNING: Failed to load {zone_file}: {e}", file=sys.stderr)
    return [], []

def is_click_in_zone(x: int, y: int, zone: dict) -> bool:
    try:
        return zone['x1'] <= x <= zone['x2'] and zone['y1'] <= y <= zone['y2']
    except:
        return False

def find_all_dirs_with_json(input_root: Path):
    if not input_root.exists() or not input_root.is_dir():
        return []
    found = set()
    for p in sorted(input_root.rglob("*")):
        if p.is_dir():
            try:
                has = any(child.is_file() and child.suffix.lower() == ".json" for child in p.iterdir())
                if has:
                    found.add(p)
            except:
                pass
    return sorted(found)

def find_json_files_in_dir(dirpath: Path):
    try:
        return sorted([p for p in dirpath.glob("*.json") if p.is_file() and not p.name.startswith("click_zones")])
    except:
        return []

def load_json_events(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"WARNING: Failed to read {path}: {e}", file=sys.stderr)
        return []
    
    if isinstance(data, dict):
        for k in ("events", "items", "entries", "records", "actions"):
            if k in data and isinstance(data[k], list):
                return deepcopy(data[k])
        if "Time" in data:
            return [deepcopy(data)]
        return []
    
    return deepcopy(data) if isinstance(data, list) else []

def process_macro_file(events: list[dict]) -> tuple[list[dict], int]:
    """Normalizes timestamps and removes redundant key events at start."""
    if not events:
        return [], 0
    
    events_with_time = []
    for idx, e in enumerate(events):
        try:
            t = int(e.get("Time", 0))
        except:
            try:
                t = int(float(e.get("Time", 0)))
            except:
                t = 0
        events_with_time.append((e, t, idx))
    
    try:
        events_with_time.sort(key=lambda x: (x[1], x[2]))
    except Exception as ex:
        print(f"WARNING: Could not sort events: {ex}", file=sys.stderr)
    
    if not events_with_time:
        return [], 0
    
    min_t = events_with_time[0][1]
    shifted = []
    for (e, t, _) in events_with_time:
        ne = deepcopy(e)
        ne["Time"] = t - min_t
        shifted.append(ne)

    first_significant_index = 0
    for i, e in enumerate(shifted):
        event_type = e.get('Type')
        if event_type in ["KeyUp", "KeyDown"]:
            first_significant_index = i + 1
            continue
        first_significant_index = i
        break
    
    cleaned_events = shifted[first_significant_index:]
    duration_ms = cleaned_events[-1]["Time"] if cleaned_events else 0
    return cleaned_events, duration_ms

def preserve_click_integrity(events):
    """Protects all click/drag/button events from time shifts."""
    preserved = []
    for i, e in enumerate(events):
        new_e = deepcopy(e)
        event_type = e.get('Type', '')
        
        is_protected_type = any(t in event_type for t in [
            'MouseDown', 'MouseUp', 'LeftDown', 'LeftUp', 'RightDown', 'RightUp', 
            'DragStart', 'DragEnd', 'Click', 'LeftClick', 'RightClick', 'Button'
        ]) 
        
        if is_protected_type:
            new_e['Time'] = int(e.get('Time', 0))
            new_e['PROTECTED'] = True
        
        preserved.append(new_e)
    
    return preserved

def is_protected_event(event):
    return event.get('PROTECTED', False)

def compute_minutes_from_ms(ms: int):
    return math.ceil(ms / 60000) if ms > 0 else 0

def number_to_letters(n: int) -> str:
    if n <= 0:
        return ""
    letters = ""
    while n > 0:
        n -= 1
        letters = chr(ord('A') + (n % 26)) + letters
        n //= 26
    return letters

def part_from_filename(path: str) -> str:
    try:
        return Path(str(path)).stem
    except:
        return str(path)

def apply_shifts(events, shift_ms):
    result = []
    for e in events:
        new_e = deepcopy(e)
        new_e['Time'] = int(e.get('Time', 0)) + int(shift_ms)
        result.append(new_e)
    return result

def merge_events_with_pauses(base_events: list[dict], new_events: list[dict], pause_ms: int) -> list[dict]:
    if not new_events:
        return base_events

    last_time = base_events[-1]['Time'] if base_events else 0
    new_macro_start_time = new_events[0].get('Time', 0)
    time_shift = last_time + pause_ms - new_macro_start_time

    shifted_events = deepcopy(new_events)
    for event in shifted_events:
        event['Time'] = event['Time'] + time_shift

    return base_events + shifted_events

def locate_special_file(folder: Path, input_root: Path):
    for cand in [folder / SPECIAL_FILENAME, input_root / SPECIAL_FILENAME]:
        if cand.exists():
            return cand.resolve()
    keyword = SPECIAL_KEYWORD.lower()
    for p in Path.cwd().rglob("*"):
        if p.is_file() and keyword in p.name.lower():
            return p.resolve()
    return None

def copy_always_files_unmodified(files, out_folder_for_group: Path):
    always_files = [f for f in files if Path(f).name.lower().startswith(("always first", "always last", "-always first", "-always last"))]
    if not always_files: return []
    copied_paths = []
    for fpath in always_files:
        fpath_obj = Path(fpath)
        dest_path = out_folder_for_group / fpath_obj.name
        try:
            shutil.copy2(fpath_obj, dest_path)
            copied_paths.append(dest_path)
            print(f"  ✓ Copied unmodified: {fpath_obj.name}")
        except Exception as e:
            print(f"  ✗ ERROR copying {fpath_obj.name}: {e}", file=sys.stderr)
    return copied_paths

# ==============================================================================
# ANTI-DETECTION & LOGIC (RESTORED PAUSES + 40% CAP)
# ==============================================================================

def calculate_afk_budget(total_event_time_ms, current_afk_time_ms):
    max_allowed = (total_event_time_ms * 2 / 3) - current_afk_time_ms
    return max(0, int(max_allowed))

def add_desktop_mouse_paths(events, rng):
    if not events: return events
    events_copy = deepcopy(events)
    click_times = []
    for i, e in enumerate(events_copy):
        if is_protected_event(preserve_click_integrity([e])[0]):
            click_times.append(int(e.get('Time', 0)))
    
    if not click_times: return events_copy
    SAFE_DISTANCE_MS = 120000
    insertions = []
    last_x, last_y = None, None
    
    for idx, e in enumerate(events_copy):
        event_type = e.get('Type', '')
        current_time = int(e.get('Time', 0))
        is_mouse_move = event_type == 'MouseMove'
        if is_mouse_move and 'X' in e and 'Y' in e:
            try:
                target_x, target_y = int(e['X']), int(e['Y'])
                if last_x is not None and last_y is not None:
                    distance = ((target_x - last_x)**2 + (target_y - last_y)**2)**0.5
                    if distance > 30:
                        min_distance_to_click = min(abs(current_time - ct) for ct in click_times)
                        if min_distance_to_click >= SAFE_DISTANCE_MS:
                            prev_time = int(events_copy[idx - 1].get('Time', 0)) if idx > 0 else 0
                            available_time = current_time - prev_time
                            num_points = rng.randint(2, 3)
                            movement_duration = min(int(100 + distance * 0.2), 300)
                            if available_time > movement_duration + 50:
                                movement_start = current_time - movement_duration
                                for i in range(1, num_points + 1):
                                    t = i / (num_points + 1)
                                    t_smooth = t * t * (3 - 2 * t)
                                    inter_x = int(last_x + (target_x - last_x) * t_smooth + rng.randint(-2, 2))
                                    inter_y = int(last_y + (target_y - last_y) * t_smooth + rng.randint(-2, 2))
                                    point_time = movement_start + int(movement_duration * t_smooth)
                                    point_time = max(prev_time + 1, min(point_time, current_time - 1))
                                    new_event = {'Time': point_time, 'Type': 'MouseMove', 'X': inter_x, 'Y': inter_y}
                                    insertions.append((idx, new_event))
                last_x, last_y = target_x, target_y
            except Exception: pass
    for insert_idx, new_event in reversed(insertions):
        events_copy.insert(insert_idx, new_event)
    return events_copy

def add_click_grace_periods(events, rng):
    return events

def add_reaction_variance(events, rng):
    varied = []
    prev_event_time = 0
    for i, e in enumerate(events):
        new_e = deepcopy(e)
        if is_protected_event(e):
            prev_event_time = int(e.get('Time', 0))
            varied.append(new_e)
            continue
        
        if any(t in e.get('Type', '') for t in ['Click', 'Down', 'Up']):
             new_e['Time'] = int(e.get('Time', 0))
             prev_event_time = int(new_e.get('Time', 0))
             varied.append(new_e)
             continue
             
        current_time = int(e.get('Time', 0))
        gap_since_last = current_time - prev_event_time
        if i > 0 and rng.random() < 0.3 and gap_since_last >= 500:
            new_e['Time'] = current_time + rng.randint(200, 600)
        prev_event_time = int(new_e.get('Time', 0))
        varied.append(new_e)
    return varied

def add_mouse_jitter(events, rng, is_desktop=False, target_zones=None, excluded_zones=None):
    if target_zones is None: target_zones = []
    if excluded_zones is None: excluded_zones = []
    jittered, jitter_range = [], [-1, 0, 1]
    for e in events:
        new_e = deepcopy(e)
        if is_protected_event(e):
            jittered.append(new_e)
            continue
        is_click = any(t in e.get('Type', '') for t in ['Click', 'LeftClick', 'DragStart', 'DragEnd', 'button'])
        if is_click and 'X' in e and 'Y' in e and e['X'] is not None:
            try:
                original_x, original_y = int(e['X']), int(e['Y'])
                in_excluded = any(is_click_in_zone(original_x, original_y, zone) for zone in excluded_zones)
                if not in_excluded and (not target_zones or any(is_click_in_zone(original_x, original_y, zone) for zone in target_zones)):
                    new_e['X'] = original_x + rng.choice(jitter_range)
                    new_e['Y'] = original_y + rng.choice(jitter_range)
            except: pass
        jittered.append(new_e)
    return jittered

def add_time_of_day_fatigue(events, rng, is_time_sensitive=False, max_pause_ms=0):
    if not events or is_time_sensitive or rng.random() < 0.20:
        return deepcopy(events), 0.0
    evs = deepcopy(events)
    if len(evs) < 2: return evs, 0.0
    
    num_pauses = rng.randint(0, 3)
    if num_pauses == 0: return evs, 0.0
    
    click_times = []
    for i, e in enumerate(evs):
        if is_protected_event(preserve_click_integrity([e])[0]):
            click_times.append((i, int(e.get('Time', 0))))
            
    safe_locations = []
    for gap_idx in range(len(evs) - 1):
        event_time = int(evs[gap_idx].get('Time', 0))
        is_safe = True
        for _, click_time in click_times:
            if (event_time - click_time) < 1000 and (event_time - click_time) > -100:
                is_safe = False
                break
        if is_safe: safe_locations.append(gap_idx)
        
    if not safe_locations: return evs, 0.0
    
    num_pauses = min(num_pauses, len(safe_locations))
    pause_locations = rng.sample(safe_locations, num_pauses)
    total_added = 0
    
    for gap_idx in sorted(pause_locations, reverse=True):
        base_pause = rng.randint(0, 72000)
        actual_pause = min(base_pause, max_pause_ms)
        if actual_pause > 0:
            for j in range(gap_idx + 1, len(evs)):
                evs[j]["Time"] = int(evs[j].get("Time", 0)) + actual_pause
            total_added += actual_pause
            max_pause_ms = max(0, max_pause_ms - actual_pause)
            
    return evs, total_added

def insert_intra_pauses(events, rng, is_time_sensitive=False, max_pause_s=33, max_num_pauses=3, max_allowed_total_afk=0):
    if not events or len(events) < 2 or not is_time_sensitive:
        return deepcopy(events), []
    evs = deepcopy(events)
    num_pauses = rng.randint(0, max_num_pauses)
    if num_pauses == 0: return evs, []
    
    click_times = []
    for i, e in enumerate(evs):
        if is_protected_event(preserve_click_integrity([e])[0]):
            click_times.append((i, int(e.get('Time', 0))))
            
    safe_locations = []
    for gap_idx in range(len(evs) - 1):
        event_time = int(evs[gap_idx].get('Time', 0))
        is_safe = True
        for _, click_time in click_times:
            if abs(event_time - click_time) < 1000:
                is_safe = False
                break
        if is_safe: safe_locations.append(gap_idx)
        
    if not safe_locations: return evs, []
    
    num_pauses = min(num_pauses, len(safe_locations))
    chosen = rng.sample(safe_locations, num_pauses)
    pauses_info = []
    
    for gap_idx in sorted(chosen):
        raw_pause = rng.randint(0, int(max_pause_s * 1000))
        pause_ms = min(raw_pause, max_allowed_total_afk)
        
        if pause_ms > 0:
            for j in range(gap_idx+1, len(evs)):
                evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
            pauses_info.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
            max_allowed_total_afk = max(0, max_allowed_total_afk - pause_ms)
            
    return evs, pauses_info

def add_afk_pause(events, rng, max_allowed_ms):
    if not events or max_allowed_ms <= 0:
        return deepcopy(events), 0
    evs = deepcopy(events)
    
    if rng.random() < 0.7:
        afk_seconds = rng.randint(60, 300)
    else:
        afk_seconds = rng.randint(300, 1200)
        
    afk_ms = afk_seconds * 1000
    afk_ms = min(afk_ms, max_allowed_ms)
    
    if afk_ms <= 0:
        return evs, 0
        
    insert_idx = rng.randint(len(evs) // 4, 3 * len(evs) // 4) if len(evs) > 1 else 0
    for j in range(insert_idx, len(evs)):
        evs[j]["Time"] = int(evs[j].get("Time", 0)) + afk_ms
        
    return evs, afk_ms
    
# ==============================================================================
# SELECTOR & MAIN LOGIC (GLOBAL POOL & STRICT UNIQUE SELECTION)
# ==============================================================================

class GlobalFileSelector:
    def __init__(self, rng, all_files):
        self.rng = rng
        self.all_files = all_files
        self.global_used_files = set() # Tracks usage across ALL merged versions
        self.current_pool = list(all_files) # Active pool for selection
        self.rng.shuffle(self.current_pool)
        
        # Calculate base durations once
        self.file_durations = {}
        for f in all_files:
            try:
                evs = load_json_events(Path(f))
                _, base_dur = process_macro_file(evs) 
                self.file_durations[f] = base_dur / 60000.0 # minutes
            except:
                self.file_durations[f] = 2.0 # fallback

    def get_files_for_time(self, target_minutes):
        """
        Selects unique files for ONE merged version to meet target time.
        - Prioritizes using unused files from the global pool.
        - strictly avoids reusing a file within the same merge unless pool is exhausted.
        """
        selected_for_this_merge = []
        estimated_total = 0.0
        AVG_INTER_FILE_PAUSE_MIN = 0.08
        
        # Local set to prevent duplicates in THIS single merged file
        used_in_this_merge = set()
        
        while estimated_total < target_minutes * 0.9:
            # 1. Try to pick from current pool (globally unused)
            candidates = [f for f in self.current_pool if f not in used_in_this_merge]
            
            if not candidates:
                # If current pool is empty, refill global pool (reset cycle)
                self.global_used_files.clear()
                self.current_pool = list(self.all_files)
                self.rng.shuffle(self.current_pool)
                candidates = [f for f in self.current_pool if f not in used_in_this_merge]
                
                if not candidates:
                    candidates = list(self.all_files) # Fallback: allow duplicates everywhere
            
            if not candidates:
                break
            
            # Smart selection logic: pick best fit for time if close
            remaining_gap = target_minutes - estimated_total
            best_fit = None
            
            if remaining_gap < 15:
                # Try to find a file that fits the gap
                fitting_candidates = [f for f in candidates if self.file_durations.get(f, 2.0) <= remaining_gap + 5]
                if fitting_candidates:
                    best_fit = self.rng.choice(fitting_candidates)
                elif candidates:
                    # If all too big, pick smallest
                    best_fit = min(candidates, key=lambda f: self.file_durations.get(f, 2.0))
            
            if not best_fit and candidates:
                best_fit = self.rng.choice(candidates)
                
            if best_fit:
                selected_for_this_merge.append(best_fit)
                dur = self.file_durations.get(best_fit, 2.0)
                estimated_total += dur + AVG_INTER_FILE_PAUSE_MIN
                
                used_in_this_merge.add(best_fit)
                self.global_used_files.add(best_fit)
                
                # Remove from current available pool if present
                if best_fit in self.current_pool:
                    self.current_pool.remove(best_fit)
            else:
                break
                
        return selected_for_this_merge


def generate_version_for_folder(rng, version_num, exclude_count, within_max_s, within_max_pauses, between_max_s, folder_path: Path, input_root: Path, global_selector, exemption_config: dict = None, target_minutes=25, max_files_per_version=None):
    """Generate merged version with random exclusion and detailed individual manifest."""
    
    # 1. Select files using the GlobalFileSelector based on target time
    selected_files = global_selector.get_files_for_time(target_minutes)
    
    if not selected_files:
        return None, [], [], {}, [], 0, "", 0.0 
    
    # 2. Random Exclusion based on exclude_count (Smart Adjustment Logic Restored)
    
    # Exclude files marked 'always first'/'always last' from random exclusion pool
    always_first_last = [f for f in selected_files if Path(f).name.lower().startswith(("always first", "always last", "-always first", "-always last"))]
    
    # Files eligible for exclusion (i.e., regular files)
    eligible_for_exclusion = [f for f in selected_files if f not in always_first_last]

    n_total = len(eligible_for_exclusion)
    
    if n_total <= 3:
        # If 3 or fewer regular files, exclude none.
        files_to_exclude_count = 0
    else:
        # Calculate safe limit: max files we can exclude while keeping at least 3
        safe_limit = max(0, n_total - 3)
        # Actual exclusion count is the lesser of the user's input and the safe limit
        files_to_exclude_count = min(exclude_count, safe_limit)

    files_to_exclude = rng.sample(eligible_for_exclusion, files_to_exclude_count)
    
    final_selected_files = [f for f in selected_files if f not in files_to_exclude]
    excluded_list = [Path(f).name for f in files_to_exclude]
    
    if not final_selected_files:
        return None, [], [], {}, excluded_list, 0, "", 0.0

    # Sort final selected files to ensure 'always first' is first, etc.
    final_selected_files.sort(key=lambda f: (
        0 if Path(f).name.lower().startswith("always first") else 
        2 if Path(f).name.lower().startswith("always last") else 
        1
    ))

    # Determine special file usage and load it first if present
    special_file = locate_special_file(folder_path, input_root)
    special_file_used = False
    
    if special_file and special_file.resolve() in [Path(f).resolve() for f in final_selected_files]:
        final_selected_files.remove(str(special_file.resolve()))
        final_selected_files.insert(0, str(special_file.resolve()))
        special_file_used = True

    # Limit file count if max_files_per_version is set
    if max_files_per_version and len(final_selected_files) > max_files_per_version:
        # Keep 'always first'/'always last'/special_file if present, then take a random sample
        preserved_files = [f for f in final_selected_files if Path(f).name.lower().startswith(("always first", "always last", SPECIAL_KEYWORD))]
        
        remaining_slots = max_files_per_version - len(preserved_files)
        if remaining_slots > 0:
            removable = [f for f in final_selected_files if f not in preserved_files]
            
            # If the removable list is too long, sample from it
            if len(removable) > remaining_slots:
                extra_excluded = rng.sample(removable, len(removable) - remaining_slots)
                final_selected_files = [f for f in final_selected_files if f not in extra_excluded]
                excluded_list.extend([Path(f).name for f in extra_excluded])
            
        else:
             # Case where max_files_per_version is too small to even hold preserved files
             final_selected_files = preserved_files[:max_files_per_version]
             
        # Re-sort to maintain file order integrity after exclusion
        final_selected_files.sort(key=lambda f: (
            0 if Path(f).name.lower().startswith("always first") else 
            2 if Path(f).name.lower().startswith("always last") else 
            1
        ))

    # --- Merging Events ---
    all_events = []
    manifest_parts = {}
    total_duration_ms = 0
    total_pause_ms = 0
    
    is_time_sensitive = is_time_sensitive_folder(folder_path)
    exemption = exemption_config or {}
    disable_intra_pauses = exemption.get("disable_intra_pauses", False) or (is_time_sensitive and exemption.get("auto_detect_time_sensitive"))
    disable_inter_pauses = exemption.get("disable_inter_pauses", False) or (is_time_sensitive and exemption.get("auto_detect_time_sensitive"))
    
    letter_count = 1
    
    for i, file_path_str in enumerate(final_selected_files):
        file_path = Path(file_path_str)
        events, macro_duration_ms = process_macro_file(load_json_events(file_path))
        
        if not events: continue

        file_added_afk = 0

        if not is_special:
            is_desktop = "deskt" in str(folder_path).lower()
            zb_evs = preserve_click_integrity(events)
            
            # --- Anti-Detection ---
            if not is_desktop:
                zb_evs = add_mouse_jitter(zb_evs, rng, is_desktop=False)
                zb_evs = add_reaction_variance(zb_evs, rng)
                if not is_time_sensitive:
                    # Calculate budget
                    budget = calculate_afk_budget(total_duration_ms, total_pause_ms)
                    zb_evs, added_afk = add_time_of_day_fatigue(zb_evs, rng, is_time_sensitive=False, max_pause_ms=budget)
                    file_added_afk += added_afk
            else:
                zb_evs = add_mouse_jitter(zb_evs, rng, is_desktop=True)
                zb_evs = add_desktop_mouse_paths(zb_evs, rng)
                zb_evs = add_reaction_variance(zb_evs, rng)
                if not is_time_sensitive:
                    budget = calculate_afk_budget(total_duration_ms, total_pause_ms)
                    zb_evs, added_afk = add_time_of_day_fatigue(zb_evs, rng, is_time_sensitive=False, max_pause_ms=budget)
                    file_added_afk += added_afk
            
            zb_evs, _ = process_macro_file(zb_evs) # Re-normalize
            
            # --- Intra-Pauses & AFK ---
            if is_time_sensitive:
                intra_evs = zb_evs
                if not exemption_config.get("disable_intra_pauses", False):
                    budget = calculate_afk_budget(total_duration_ms, total_pause_ms)
                    intra_evs, added_info = insert_intra_pauses(zb_evs, rng, True, within_max_s, within_max_pauses, budget)
                    added_ms = sum(p['pause_ms'] for p in added_info)
                    file_added_afk += added_ms
            else:
                intra_evs = zb_evs
            
            # AFK Pause (Random 50% chance)
            if rng.random() < 0.5:
                budget = calculate_afk_budget(total_duration_ms, total_pause_ms)
                intra_evs, added_afk = add_afk_pause(intra_evs, rng, budget)
                file_added_afk += added_afk
        else:
            intra_evs = events
        
        events = intra_evs
        
        # Inter-file pause logic
        pause_ms = 0
        if i > 0 and not disable_inter_pauses:
            budget = calculate_afk_budget(total_duration_ms, total_pause_ms)
            if is_time_sensitive and exemption_config.get("disable_inter_pauses", False):
                raw_pause = rng.randint(100, 500)
            elif is_time_sensitive:
                raw_pause = rng.randint(0, int(between_max_s * 1000))
            else:
                raw_pause = rng.randint(500, between_max_s * 1000)
            
            pause_ms = min(raw_pause, budget)
            
        total_pause_ms += pause_ms + file_added_afk
        all_events = merge_events_with_pauses(all_events, events, pause_ms)
        
        part_letter = number_to_letters(letter_count)
        
        final_file_duration = macro_duration_ms + file_added_afk + pause_ms
        final_file_mins = round(final_file_duration / 60000.0)
        file_stats_list.append(f"{part_from_filename(fpath_obj)} (~ {final_file_mins} mins)")
        
        # Manifest entry
        manifest_parts[part_letter] = {
            "file": file_path.name,
            "duration_m": macro_duration_ms / 60000.0,
            "start_time_ms": all_events[-len(events)]['Time'] if events else 0,
            "end_time_ms": all_events[-1]['Time'] if all_events else 0,
            "inter_pause_ms": pause_ms
        }
        
        letter_count += 1
        total_duration_ms += macro_duration_ms
    
    # Recalculate duration based on actual events
    final_event_time = all_events[-1]['Time'] if all_events else 0
    total_minutes = final_event_time / 60000.0
    total_afk_minutes = round(total_pause_ms / 60000.0, 1)
    
    # --- Final Output Naming ---
    version_char = number_to_letters(version_num)
    
    # List all parts used
    parts_list_str = " - ".join([f"{l}[{Path(p['file']).stem}]" for l, p in manifest_parts.items()])
    
    # Special file name handling
    if special_file_used and SPECIAL_FILENAME in parts_list_str:
        parts_list_str = parts_list_str.replace(SPECIAL_FILENAME, SPECIAL_KEYWORD)
    
    # Folder name
    folder_name = folder_path.name.replace(" ", "")
    
    merged_fname = f"{folder_name}_{version_char}_{int(total_minutes)}m={parts_list_str}.json"
    
    # Compile the final manifest
    final_manifest = {
        "folder_group": folder_path.name,
        "merged_version": version_num,
        "total_duration_m": total_minutes,
        "macro_events_m": (total_duration_ms) / 60000.0, 
        "total_pause_m": total_pause_ms / 60000.0,
        "afk_percentage": (total_pause_ms / final_event_time) * 100 if final_event_time > 0 else 0,
        "is_time_sensitive": is_time_sensitive,
        "parts": manifest_parts,
        "excluded_files": excluded_list
    }

    # --- CREATE SINGLE MANIFEST ENTRY CONTENT ---
    # File Name at top
    manifest_entry = f"Merged Version: {merged_fname}\n" # Fixed to show full merged name
    manifest_entry += f"Total Duration: {int(total_minutes)} mins\n"
    manifest_entry += f"Total Pause/AFK Time: {total_afk_minutes} mins\n"
    manifest_entry += f"File Count: {len(final_files)}\n"
    manifest_entry += f"Target Time: {target_minutes} mins\n"
    manifest_entry += "\nFiles Used:\n"
    for i, stat in enumerate(file_stats_list):
        manifest_entry += f"{i+1}. {stat}\n"
    
    safe_name = ''.join(ch for ch in merged_fname if ch not in '/\\:*?"<>|')
    excluded = excluded_list 
    
    return safe_name, all_events, [final_manifest], manifest_parts, excluded, total_minutes, manifest_entry, total_afk_minutes

def main():
    parser = argparse.ArgumentParser(description="Merge OSRS macro events for anti-detection.")
    parser.add_argument("input_root", type=Path, help="Root directory containing macro folders.")
    parser.add_argument("output_root", type=Path, help="Root directory for merged output files.")
    parser.add_argument("--versions", type=int, default=6, help="How many versions per group.")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--exclude-count", type=int, default=10, help="Max files to randomly exclude per version.")
    parser.add_argument("--within-max-time", dest="within_max_s", type=int, default=33, help="Intra-file max pause time (seconds).")
    parser.add_argument("--within-max-pauses", type=int, default=3, help="Max intra-file pauses (0-3 randomly chosen).")
    parser.add_argument("--between-max-time", dest="between_max_s", type=int, default=18, help="Inter-file max pause time (seconds).")
    parser.add_argument("--target-minutes", type=int, default=25, help="Target duration per merged file in minutes.")
    parser.add_argument("--max-files", type=int, default=None, help="Max number of individual files to use per merged version.")
    args = parser.parse_args()

    # --- Setup ---
    rng = random.Random()
    rng.seed(os.urandom(10)) 
    
    bundle_n = read_counter(COUNTER_PATH)
    output_base_name = f"merged_bundle_{bundle_n}"
    output_parent = args.output_root
    
    print(f"Starting merge for bundle {bundle_n}...")
    print(f"Config: Target={args.target_minutes}m, Versions={args.versions}, Exclude={args.exclude_count}")

    # --- Find Groups and Files ---
    folders = find_all_dirs_with_json(args.input_root)
    if not folders:
        print(f"No folders with JSON files found in {args.input_root}", file=sys.stderr)
        return
        
    print(f"Found {len(folders)} macro groups.")
    
    all_files_for_pool = []
    for f in folders:
        all_files_for_pool.extend(find_json_files_in_dir(f))
        
    global_selector = GlobalFileSelector(rng, all_files_for_pool)
    exemption_config = load_exemption_config()
    
    all_written_paths = []
    
    # --- Processing ---
    
    # Create the output structure within the bundle folder
    output_bundle_root = output_parent / output_base_name
    output_bundle_root.mkdir(parents=True, exist_ok=True)
    
    for folder in folders:
        print(f"\nProcessing group: {folder.name}")
        files = find_json_files_in_dir(folder)
        if not files:
            print("  Skipping: No files found in folder.")
            continue
            
        # Create a folder for the specific group inside the bundle
        out_folder_for_group = output_bundle_root / folder.name
        out_folder_for_group.mkdir(parents=True, exist_ok=True)
            
        # Copy 'always' files first
        always_copied = copy_always_files_unmodified(files, out_folder_for_group)
        all_written_paths.extend(always_copied)
        
        regular_files = [f for f in files if not Path(f).name.lower().startswith(("always first", "always last", "-always first", "-always last"))]
        
        # RE-INITIALIZE Global Selector for EACH folder so pools don't mix across unrelated folders
        folder_global_selector = GlobalFileSelector(rng, regular_files)
        
        for v in range(1, max(1, args.versions) + 1):
            merged_fname, merged_events, finals, pauses, excluded, total_minutes, manifest_entry, afk_total = generate_version_for_folder(
                rng, v, args.exclude_count, args.within_max_s, args.within_max_pauses, 
                args.between_max_s, folder, args.input_root, folder_global_selector, exemption_config, 
                target_minutes=args.target_minutes, max_files_per_version=args.max_files
            )
            
            if not merged_fname:
                continue
                
            out_path = out_folder_for_group / merged_fname
            
            # Create a simplified manifest file inside the output group folder
            manifest_path = out_folder_for_group / f"{Path(merged_fname).stem}_manifest.txt"
            try:
                manifest_path.write_text(manifest_entry, encoding="utf-8")
                all_written_paths.append(manifest_path)
            except Exception as e:
                print(f"  ✗ ERROR writing manifest {manifest_path}: {e}", file=sys.stderr)

            # Write the merged events file
            try:
                out_path.write_text(json.dumps(merged_events, indent=2, ensure_ascii=False), encoding="utf-8")
                print(f"  ✓ Version {v} ({int(total_minutes)}m): {out_path.name} (Excluded: {len(excluded)})")
                all_written_paths.append(out_path)
            except Exception as e:
                print(f"  ✗ ERROR writing {out_path}: {e}", file=sys.stderr)

    # --- Finalize ---
    if all_written_paths:
        print(f"\nSuccessfully generated {len(all_written_paths)} merged files.")
    else:
        print("\nNo merged files were successfully generated.", file=sys.stderr)

    # Increment counter for next run
    next_bundle_n = bundle_n + 1
    write_counter(COUNTER_PATH, next_bundle_n)
    print(f"Next bundle number saved: {next_bundle_n}")
    
if __name__ == "__main__":
    main()
