#!/usr/bin/env python3
"""merge_macros.py - OSRS Anti-Detection with AFK & Zone Awareness (Final Patch + Manifest)"""

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
        
        # Explicitly protect all click/tap interaction types
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
    """
    Returns max pause ms allowed to keep Total AFK <= 40% of Total Time.
    Formula: NewPause <= (2/3 * Events) - CurrentAFK
    """
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
    # Disabled to ensure click integrity
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
        
        # Extra check for unprotected click-like types
        if any(t in e.get('Type', '') for t in ['Click', 'Down', 'Up']):
             new_e['Time'] = int(e.get('Time', 0))
             prev_event_time = int(new_e.get('Time', 0))
             varied.append(new_e)
             continue
             
        current_time = int(e.get('Time', 0))
        gap_since_last = current_time - prev_event_time
        if i > 0 and rng.random() < 0.3 and gap_since_last >= 500:
            new_e['Time'] = current_time + rng.randint(200, 600) # Original range maintained
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
    # Original chance
    if not events or is_time_sensitive or rng.random() < 0.20:
        return deepcopy(events), 0.0
    evs = deepcopy(events)
    if len(evs) < 2: return evs, 0.0
    
    num_pauses = rng.randint(0, 3)
    if num_pauses == 0: return evs, 0.0
    
    # Identify safe spots
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
                is_safe = False # Too close to a click
                break
        if is_safe: safe_locations.append(gap_idx)
        
    if not safe_locations: return evs, 0.0
    
    num_pauses = min(num_pauses, len(safe_locations))
    pause_locations = rng.sample(safe_locations, num_pauses)
    total_added = 0
    
    for gap_idx in sorted(pause_locations, reverse=True):
        # Original max 72s
        base_pause = rng.randint(0, 72000)
        # Clamp to budget (40% constraint)
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
        # Original max_pause_s (33s default)
        raw_pause = rng.randint(0, int(max_pause_s * 1000))
        # Clamp to budget
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
    
    # Original ranges
    if rng.random() < 0.7:
        afk_seconds = rng.randint(60, 300)
    else:
        afk_seconds = rng.randint(300, 1200)
        
    afk_ms = afk_seconds * 1000
    # Clamp to budget
    afk_ms = min(afk_ms, max_allowed_ms)
    
    if afk_ms <= 0:
        return evs, 0
        
    insert_idx = rng.randint(len(evs) // 4, 3 * len(evs) // 4) if len(evs) > 1 else 0
    for j in range(insert_idx, len(evs)):
        evs[j]["Time"] = int(evs[j].get("Time", 0)) + afk_ms
        
    return evs, afk_ms
    
# ==============================================================================
# SELECTOR & MAIN LOGIC (PATCHED DURATION LOGIC & EXCLUSION & MANIFEST)
# ==============================================================================

class NonRepeatingSelector:
    def __init__(self, rng):
        self.rng = rng
        self.used_combos = set()
        self.used_files = set()
    
    def select_unique_files_for_time(self, files, target_minutes):
        """
        Selects files until the target duration is reached, reusing files if necessary.
        Prioritizes TIME over file count.
        """
        if not files:
            return []
            
        file_durations = {}
        for f in files:
            try:
                evs = load_json_events(Path(f))
                _, base_dur = process_macro_file(evs) 
                file_durations[f] = base_dur / 60000.0 # minutes
            except:
                file_durations[f] = 2.0 # fallback
        
        # Inter-file pause average (approx 5 sec = 0.08 min)
        AVG_INTER_FILE_PAUSE_MIN = 0.08 
        
        selected = []
        estimated_total = 0.0
        
        # Create a working pool
        available_pool = [f for f in files if f not in self.used_files]
        if not available_pool:
            self.used_files.clear()
            available_pool = list(files)
            
        # Loop until we hit the target time
        while estimated_total < target_minutes:
            if not available_pool:
                # Refill pool if empty to allow reuse
                available_pool = list(files)
                self.rng.shuffle(available_pool)
            
            # Pick a file
            chosen = self.rng.choice(available_pool)
            dur = file_durations.get(chosen, 2.0)
            
            selected.append(chosen)
            estimated_total += dur + AVG_INTER_FILE_PAUSE_MIN
            
            # Mark as used (locally)
            if chosen in available_pool:
                available_pool.remove(chosen)
            self.used_files.add(chosen)
            
        print(f"  Selected {len(selected)} files, estimated total: {estimated_total:.1f}m (target: {target_minutes}m)")
        return selected
    
    def shuffle_with_memory(self, items):
        if not items or len(items) <= 1:
            return items
        shuffled = items.copy()
        self.rng.shuffle(shuffled)
        return shuffled

def generate_version_for_folder(files, rng, version_num, exclude_count, within_max_s, within_max_pauses, between_max_s, folder_path: Path, input_root: Path, selector, exemption_config: dict = None, target_minutes=25, max_files_per_version=4):
    """Generate merged version with 40% AFK cap and Target Time enforcement."""
    if not files:
        return None, [], [], {"inter_file_pauses": [], "intra_file_pauses": []}, [], 0
        
    always_first = next((f for f in files if Path(f).name.lower().startswith(("always first", "-always first"))), None)
    always_last = next((f for f in files if Path(f).name.lower().startswith(("always last", "-always last"))), None)
    regular_files = [f for f in files if f not in [always_first, always_last]]
    
    if not regular_files:
        return None, [], [], {}, [], 0
    
    # --- SMART EXCLUSION LOGIC ---
    # If total available files are few (<= 5), adjust exclusion to ensure 
    # we don't deplete the pool too much (leave at least 3 files or exclude 0 if very few).
    n_total = len(regular_files)
    if n_total <= 3:
        pool_for_version = regular_files
    else:
        # Allow exclusion, but ensure we keep at least 2-3 files in the pool
        safe_limit = max(0, n_total - 3)
        actual_exclude = min(exclude_count, safe_limit)
        if actual_exclude > 0:
            excluded_subset = rng.sample(regular_files, actual_exclude)
            pool_for_version = [f for f in regular_files if f not in excluded_subset]
        else:
            pool_for_version = regular_files

    # --- 1. Select files to meet target duration ---
    selected_files = selector.select_unique_files_for_time(pool_for_version, target_minutes)
    
    if not selected_files:
        # Fallback if selection failed
        return None, [], [], {}, [], 0
        
    final_files = selector.shuffle_with_memory(selected_files)
    
    # Re-insert always first/last if they exist
    if always_first: final_files.insert(0, always_first)
    if always_last: final_files.append(always_last)

    # Insert special file for mobile
    special_path = locate_special_file(folder_path, input_root)
    is_mobile_group = any("mobile" in part.lower() for part in folder_path.parts)
    if is_mobile_group and special_path is not None:
        final_files = [f for f in final_files if Path(f).resolve() != special_path.resolve()]
        mid_idx = len(final_files) // 2
        final_files.insert(mid_idx, str(special_path))

    target_zones, excluded_zones = load_click_zones(folder_path)
    merged, pause_info, time_cursor = [], {"inter_file_pauses": [], "intra_file_pauses": []}, 0
    per_file_event_ms, per_file_inter_ms = {}, {}
    exemption_config = exemption_config or {"auto_detect_time_sensitive": True, "disable_intra_pauses": False, "disable_inter_pauses": False}
    is_time_sensitive = is_time_sensitive_folder(folder_path)
    
    # Track accumulators for 40% AFK Logic
    total_events_duration_so_far = 0
    total_afk_duration_so_far = 0
    
    for idx, fpath in enumerate(final_files):
        if fpath is None: continue
        fpath_obj = Path(fpath)
        is_special = special_path is not None and fpath_obj.resolve() == special_path.resolve()
        
        raw_evs = load_json_events(fpath_obj)
        zb_evs, file_duration_ms = process_macro_file(raw_evs)
        
        # Update event duration accumulator
        total_events_duration_so_far += file_duration_ms
        
        if not is_special:
            is_desktop = "deskt" in str(folder_path).lower()
            zb_evs = preserve_click_integrity(zb_evs)
            
            # --- Anti-Detection ---
            if not is_desktop:
                zb_evs = add_mouse_jitter(zb_evs, rng, is_desktop=False, target_zones=target_zones, excluded_zones=excluded_zones)
                zb_evs = add_reaction_variance(zb_evs, rng)
                if not is_time_sensitive:
                    # Calculate budget
                    budget = calculate_afk_budget(total_events_duration_so_far, total_afk_duration_so_far)
                    zb_evs, added_afk = add_time_of_day_fatigue(zb_evs, rng, is_time_sensitive=False, max_pause_ms=budget)
                    total_afk_duration_so_far += added_afk
            else:
                zb_evs = add_mouse_jitter(zb_evs, rng, is_desktop=True, target_zones=target_zones, excluded_zones=excluded_zones)
                zb_evs = add_desktop_mouse_paths(zb_evs, rng)
                zb_evs = add_reaction_variance(zb_evs, rng)
                if not is_time_sensitive:
                    budget = calculate_afk_budget(total_events_duration_so_far, total_afk_duration_so_far)
                    zb_evs, added_afk = add_time_of_day_fatigue(zb_evs, rng, is_time_sensitive=False, max_pause_ms=budget)
                    total_afk_duration_so_far += added_afk
            
            zb_evs, _ = process_macro_file(zb_evs) # Re-normalize
            
            # --- Intra-Pauses & AFK ---
            if is_time_sensitive:
                intra_evs = zb_evs
                if not exemption_config.get("disable_intra_pauses", False):
                    budget = calculate_afk_budget(total_events_duration_so_far, total_afk_duration_so_far)
                    intra_evs, added_info = insert_intra_pauses(zb_evs, rng, True, within_max_s, within_max_pauses, budget)
                    total_afk_duration_so_far += sum(p['pause_ms'] for p in added_info)
            else:
                intra_evs = zb_evs
            
            # AFK Pause (Random 50% chance)
            if rng.random() < 0.5:
                budget = calculate_afk_budget(total_events_duration_so_far, total_afk_duration_so_far)
                intra_evs, added_afk = add_afk_pause(intra_evs, rng, budget)
                total_afk_duration_so_far += added_afk
        else:
            intra_evs = zb_evs
            
        # Merge
        per_file_event_ms[str(fpath_obj)] = intra_evs[-1]["Time"] if intra_evs else 0
        
        # --- Inter-File Pause with Budget ---
        pause_ms = 0
        if idx > 0:
            budget = calculate_afk_budget(total_events_duration_so_far, total_afk_duration_so_far)
            if is_time_sensitive and exemption_config.get("disable_inter_pauses", False):
                raw_pause = rng.randint(100, 500)
            elif is_time_sensitive:
                raw_pause = rng.randint(0, int(between_max_s * 1000))
            else:
                # Original inter-file pause: 1000-12000
                raw_pause = rng.randint(1000, 12000)
            
            pause_ms = min(raw_pause, budget)
            total_afk_duration_so_far += pause_ms

        merged = merge_events_with_pauses(merged, intra_evs, pause_ms)
        time_cursor = merged[-1]["Time"] if merged else time_cursor
        
        if idx < len(final_files) - 1:
            per_file_inter_ms[str(fpath_obj)] = pause_ms
            pause_info["inter_file_pauses"].append({"after_file": fpath_obj.name, "pause_ms": pause_ms})
        else:
            per_file_inter_ms[str(fpath_obj)] = 1000
            time_cursor += 1000
    
    total_ms = time_cursor if merged else 0
    total_minutes = compute_minutes_from_ms(total_ms)
    
    # Construct filename
    parts = []
    # Create a cleaner filename (no full path details)
    letters = number_to_letters(version_num or 1)
    base_name = f"{letters}_{total_minutes}m_{len(final_files)}files"
    
    # --- CREATE MANIFEST FILE CONTENT ---
    # This will be written as a separate .txt file in the output folder
    manifest_content = f"Merged Version: {base_name}\nTotal Duration: {total_minutes} mins\nFile Count: {len(final_files)}\n\nFiles Used:\n"
    for i, f in enumerate(final_files):
        if f:
            manifest_content += f"{i+1}. {part_from_filename(f)}\n"
    
    safe_name = ''.join(ch for ch in base_name if ch not in '/\\:*?"<>|')
    excluded = [f for f in regular_files if f not in selected_files]
    
    # Return manifest content as an extra item in the tuple
    return f"{safe_name}.json", merged, [str(p) for p in final_files], pause_info, [str(p) for p in excluded], total_minutes, manifest_content

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="originals")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--versions", type=int, default=26)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--exclude-count", type=int, default=10)
    parser.add_argument("--within-max-time", default="33")
    parser.add_argument("--within-max-pauses", type=int, default=2)
    parser.add_argument("--between-max-time", default="18")
    parser.add_argument("--target-minutes", type=int, default=25)
    parser.add_argument("--max-files", type=int, default=4)
    args = parser.parse_args()
    rng = random.Random(args.seed) if args.seed is not None else random.Random()
    input_root, output_parent = Path(args.input_dir), Path(args.output_dir)
    output_parent.mkdir(parents=True, exist_ok=True)
    
    current_bundle_seq = int(os.environ.get("BUNDLE_SEQ", "").strip() or read_counter(COUNTER_PATH) or 1)
    output_base_name, output_root = f"merged_bundle_{current_bundle_seq}", output_parent / f"merged_bundle_{current_bundle_seq}"
    output_root.mkdir(parents=True, exist_ok=True)
    
    folder_dirs = find_all_dirs_with_json(input_root)
    if not folder_dirs:
        print(f"No JSON files found in {input_root}", file=sys.stderr)
        write_counter(COUNTER_PATH, current_bundle_seq + 1)
        return
        
    try:
        within_max_s = parse_time_to_seconds(args.within_max_time)
        between_max_s = parse_time_to_seconds(args.between_max_time)
    except Exception as e:
        print(f"ERROR parsing time: {e}", file=sys.stderr)
        write_counter(COUNTER_PATH, current_bundle_seq + 1)
        return
        
    all_written_paths = []
    exemption_config = load_exemption_config()
    for folder in folder_dirs:
        files = find_json_files_in_dir(folder)
        if not files: continue
        try:
            rel_folder = folder.relative_to(input_root)
        except:
            rel_folder = Path(folder.name)
        out_folder_for_group = output_root / rel_folder
        out_folder_for_group.mkdir(parents=True, exist_ok=True)
        selector = NonRepeatingSelector(rng)
        
        print(f"\nProcessing folder: {rel_folder}")
        always_copied = copy_always_files_unmodified(files, out_folder_for_group)
        all_written_paths.extend(always_copied)
        
        for v in range(1, max(1, args.versions) + 1):
            # Unpack the new manifest_content return value
            merged_fname, merged_events, finals, pauses, excluded, total_minutes, manifest_content = generate_version_for_folder(
                files, rng, v, args.exclude_count, within_max_s, args.within_max_pauses, 
                between_max_s, folder, input_root, selector, exemption_config, 
                target_minutes=args.target_minutes, max_files_per_version=args.max_files
            )
            if not merged_fname: continue
            
            # Write the JSON merged file
            out_path = out_folder_for_group / merged_fname
            try:
                out_path.write_text(json.dumps(merged_events, indent=2, ensure_ascii=False), encoding="utf-8")
                print(f"  ✓ Version {v}: {merged_fname} ({total_minutes}m)")
                all_written_paths.append(out_path)
                
                # --- WRITE MANIFEST FILE ---
                # Create a .txt file with the same base name as the json
                manifest_path = out_folder_for_group / f"{Path(merged_fname).stem}_manifest.txt"
                manifest_path.write_text(manifest_content, encoding="utf-8")
                all_written_paths.append(manifest_path)
                
            except Exception as e:
                print(f"  ✗ ERROR writing {out_path}: {e}", file=sys.stderr)
                
    zip_path = output_parent / f"{output_base_name}.zip"
    with ZipFile(zip_path, "w") as zf:
        for fpath in all_written_paths:
            try:
                arcname = str(fpath.relative_to(output_parent))
            except:
                arcname = f"{output_base_name}/{fpath.name}"
            zf.write(fpath, arcname=arcname)
            
    write_counter(COUNTER_PATH, current_bundle_seq + 1)
    print(f"\n✅ DONE. Created: {zip_path} ({len(all_written_paths)} files)")

if __name__ == "__main__":
    main()
