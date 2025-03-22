import os
import math
import mmap
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

# Global memory map variables (for optimization 3)
_global_mm = None
_global_file_size = 0

def round_up(num):
    """Round up to one decimal place."""
    return math.ceil(num * 10) / 10

def default_city_stats():
    # [min_score, max_score, total_score, count]
    return [float('inf'), float('-inf'), 0.0, 0]

def process_lines(lines):
    """Process a list of lines and update city statistics."""
    local_stats = defaultdict(default_city_stats)
    for line in lines:
        if not line:
            continue
        try:
            city, score_str = line.split(b';', 1)
            score = float(score_str)
        except Exception:
            continue
        stats = local_stats[city]
        stats[0] = min(stats[0], score)
        stats[1] = max(stats[1], score)
        stats[2] += score
        stats[3] += 1
    return local_stats

def merge_dicts(dict_list):
    """Merge a list of dictionaries into one."""
    merged = defaultdict(default_city_stats)
    for d in dict_list:
        for city, stats in d.items():
            agg = merged[city]
            agg[0] = min(agg[0], stats[0])
            agg[1] = max(agg[1], stats[1])
            agg[2] += stats[2]
            agg[3] += stats[3]
    return merged

def process_chunk(filename, start, end, thread_count):
    """
    Process a file chunk given by byte offsets [start, end) using threads.
    Uses the global memory map if available.
    """
    global _global_mm, _global_file_size
    if _global_mm is not None:
        mm = _global_mm
        file_size = _global_file_size
    else:
        with open(filename, 'rb') as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            file_size = len(mm)

    # Adjust start: if not at the beginning, jump to the next newline
    if start != 0:
        while start < file_size and mm[start] != ord('\n'):
            start += 1
        start += 1

    # Adjust end to include the full last line
    while end < file_size and mm[end] != ord('\n'):
        end += 1
    if end < file_size:
        end += 1

    chunk = mm[start:end]
    # Only close if the mapping was opened locally (not inherited)
    if _global_mm is None:
        mm.close()

    # Split the chunk into lines and partition for threads
    lines = chunk.split(b'\n')
    total_lines = len(lines)
    if total_lines == 0:
        return defaultdict(default_city_stats)
    part_size = (total_lines + thread_count - 1) // thread_count
    partitions = [lines[i * part_size: (i + 1) * part_size] for i in range(thread_count)]

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [executor.submit(process_lines, part) for part in partitions]
        thread_results = [future.result() for future in futures]

    return merge_dicts(thread_results)

def merge_process_results(results):
    """Merge dictionaries returned by processes."""
    final_stats = defaultdict(default_city_stats)
    for d in results:
        for city, stats in d.items():
            agg = final_stats[city]
            agg[0] = min(agg[0], stats[0])
            agg[1] = max(agg[1], stats[1])
            agg[2] += stats[2]
            agg[3] += stats[3]
    return final_stats

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    global _global_mm, _global_file_size

    # Create a global memory map (optimization 3)
    with open(input_file_name, 'rb') as f:
        _global_mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        _global_file_size = len(_global_mm)
    file_size = _global_file_size

    # Dynamic tuning (optimization 6): if the file is small, avoid parallelism overhead.
    cores = os.cpu_count() or 1
    threshold = 10 * 1024 * 1024  # 10 MB threshold
    if file_size < threshold:
        process_count = 1
        thread_count = 1
    else:
        process_count = 4 if cores >= 4 else cores
        thread_count = 2 if cores > 1 else 1

    # Calculate byte offsets for each process chunk
    chunk_size = file_size // process_count
    chunks = [(i * chunk_size, (i + 1) * chunk_size if i < process_count - 1 else file_size)
              for i in range(process_count)]

    # Use a multiprocessing Pool; on Unix, the fork start method ensures that
    # the global memory map is inherited by child processes.
    with multiprocessing.Pool(processes=process_count) as pool:
        params = [(input_file_name, start, end, thread_count) for start, end in chunks]
        process_results = pool.starmap(process_chunk, params)

    # Merge results from all processes (optimization 4)
    final_stats = merge_process_results(process_results)

    # Close the global mapping
    _global_mm.close()
    _global_mm = None

    # Write the results (sorted by city name)
    with open(output_file_name, 'w') as out_file:
        for city in sorted(final_stats.keys()):
            mn, mx, total, count = final_stats[city]
            if count > 0:
                avg = round_up(total / count)
                out_file.write(f"{city.decode()}={round_up(mn):.1f}/{avg:.1f}/{round_up(mx):.1f}\n")

if __name__ == "__main__":
    main()
