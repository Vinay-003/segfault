import os
import math
import mmap
from collections import defaultdict
import concurrent.futures

# ---------------------
# Top-level helper functions

def round_up(x, digits=1):
    """Rounds x upward (toward +∞) to the specified number of decimal places."""
    factor = 10 ** digits
    return math.ceil(x * factor) / factor

def default_city_data():
    # [min, max, sum, count]
    return [float('inf'), float('-inf'), 0.0, 0]

def merge_stats(stats1, stats2):
    """Merge two stats dictionaries: city -> [min, max, sum, count]."""
    for city, data in stats2.items():
        if city in stats1:
            m, M, s, cnt = stats1[city]
            dm, dM, ds, dcnt = data
            stats1[city] = [min(m, dm), max(M, dM), s + ds, cnt + dcnt]
        else:
            stats1[city] = data
    return stats1

# ---------------------
# Subchunk processing using multithreading

def process_subchunk(lines):
    """
    Process a list of decoded lines (strings).
    Returns a dictionary mapping city -> [min, max, sum, count].
    """
    stats = defaultdict(default_city_data)
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # Use partition to split at the first semicolon.
        city, sep, score_str = line.partition(";")
        if not sep:
            continue  # Skip if no semicolon found.
        city = city.strip()
        score_str = score_str.strip()
        try:
            score = float(score_str)
        except ValueError:
            continue
        entry = stats[city]
        if entry[0] == float('inf'):
            stats[city] = [score, score, score, 1]
        else:
            entry[0] = min(entry[0], score)
            entry[1] = max(entry[1], score)
            entry[2] += score
            entry[3] += 1
    return stats

def process_chunk_lines(lines, num_threads):
    """
    Splits lines into subchunks and processes them concurrently using multithreading.
    Returns a dictionary mapping city -> [min, max, sum, count].
    """
    if len(lines) < num_threads:
        return process_subchunk(lines)
    subchunk_size = max(1, len(lines) // num_threads)
    subchunks = [lines[i:i+subchunk_size] for i in range(0, len(lines), subchunk_size)]
    combined_stats = defaultdict(default_city_data)
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = executor.map(process_subchunk, subchunks)
    for subdict in results:
        merge_stats(combined_stats, subdict)
    return combined_stats

# ---------------------
# Multiprocessing: Process a file chunk using mmap

def process_file_chunk(filename, start, end, num_threads):
    with open(filename, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            mm.seek(start)
            chunk_data = mm.read(end - start)
    try:
        decoded = chunk_data.decode('utf-8', errors='replace')
    except Exception:
        decoded = ""
    lines = decoded.splitlines()
    return process_chunk_lines(lines, num_threads)

# Top-level helper to unpack arguments (to avoid lambda)
def process_file_chunk_wrapper(args):
    return process_file_chunk(*args)

def get_chunk_boundaries(filename, num_chunks):
    file_size = os.path.getsize(filename)
    chunk_size = file_size // num_chunks
    boundaries = []
    with open(filename, "rb") as f:
        start = 0
        for i in range(num_chunks):
            f.seek(start + chunk_size)
            line = f.readline()  # read until newline
            if not line:
                end = file_size
            else:
                end = f.tell()
            boundaries.append((start, end))
            start = end
        if boundaries:
            boundaries[-1] = (boundaries[-1][0], file_size)
    return boundaries

# ---------------------
# Main Function

def main(input_file="testcase.txt", output_file="output.txt"):
    num_processes = os.cpu_count() or 1
    num_threads = os.cpu_count() or 1  # Use same number of threads per process
    
    boundaries = get_chunk_boundaries(input_file, num_processes)
    overall_stats = defaultdict(default_city_data)
    
    tasks = [(input_file, start, end, num_threads) for (start, end) in boundaries]
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_processes) as executor:
        results = executor.map(process_file_chunk_wrapper, tasks)
    
    for chunk_stats in results:
        merge_stats(overall_stats, chunk_stats)
    
    with open(output_file, "w", encoding="utf-8") as fout:
        for city in sorted(overall_stats.keys()):
            m, M, s, cnt = overall_stats[city]
            avg = s / cnt
            # City is already a string, so no need to decode.
            fout.write(f"{city}={round_up(m):.1f}/{round_up(avg):.1f}/{round_up(M):.1f}\n")

if __name__ == "__main__":
    main()
