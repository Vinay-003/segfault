import os
import math
import mmap
from collections import defaultdict
import concurrent.futures

# ---------------------
# Top-level helper functions

def default_stats():
    """Return a default stats list: [min, max, sum, count]."""
    return [None, None, 0.0, 0]

def round_up(x, digits=1):
    """
    Rounds x upward (toward +∞) to the specified number of decimal places.
    For example:
      round_up(-0.15, 1) returns -0.1
      round_up(2.341, 1) returns 2.4
    """
    factor = 10 ** digits
    return math.ceil(x * factor) / factor

def merge_stats(stats1, stats2):
    """
    Merges two dictionaries of stats.
    Each dictionary maps city -> [min, max, sum, count].
    """
    for city, data in stats2.items():
        if city in stats1:
            m, M, s, cnt = stats1[city]
            dm, dM, ds, dcnt = data
            stats1[city] = [min(m, dm), max(M, dM), s + ds, cnt + dcnt]
        else:
            stats1[city] = data
    return stats1

# ---------------------
# Subchunk processing (for multithreading)

def process_subchunk(lines):
    """
    Process a list of decoded lines (strings).
    Returns a dictionary mapping city -> [min, max, sum, count].
    """
    stats = defaultdict(default_stats)
    for line in lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split(";")
        if len(parts) != 2:
            continue  # Skip malformed lines
        city = parts[0].strip()
        try:
            value = float(parts[1].strip())
        except ValueError:
            continue
        if stats[city][0] is None:
            stats[city] = [value, value, value, 1]
        else:
            stats[city][0] = min(stats[city][0], value)
            stats[city][1] = max(stats[city][1], value)
            stats[city][2] += value
            stats[city][3] += 1
    return stats

def process_chunk_lines(lines, num_threads):
    """
    Splits lines into subchunks and processes them in parallel (multithreading).
    Returns a dictionary mapping city -> [min, max, sum, count].
    """
    # If there are fewer lines than threads, process sequentially
    if len(lines) < num_threads:
        return process_subchunk(lines)
    
    subchunk_size = max(1, len(lines) // num_threads)
    subchunks = [lines[i:i+subchunk_size] for i in range(0, len(lines), subchunk_size)]
    combined_stats = defaultdict(default_stats)
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = executor.map(process_subchunk, subchunks)
    for subdict in results:
        merge_stats(combined_stats, subdict)
    return combined_stats

# ---------------------
# Multiprocessing: Process a file chunk using mmap

def process_file_chunk(filename, start, end, num_threads):
    """
    Opens the file, memory-maps it, and processes the data in the byte range [start, end).
    Splits the chunk into lines and uses multithreading to process subchunks.
    Returns a dictionary mapping city -> [min, max, sum, count].
    """
    local_stats = defaultdict(default_stats)
    with open(filename, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            mm.seek(start)
            chunk_data = mm.read(end - start)
            # Split chunk_data into lines (as bytes)
            raw_lines = chunk_data.split(b'\n')
            # Decode each line; skip lines that cannot be decoded.
            decoded_lines = []
            for bline in raw_lines:
                try:
                    decoded_lines.append(bline.decode('utf-8'))
                except UnicodeDecodeError:
                    continue
            local_stats = process_chunk_lines(decoded_lines, num_threads)
    return local_stats

def get_chunk_boundaries(filename, num_chunks):
    """
    Divides the file into num_chunks chunks.
    Returns a list of (start, end) byte positions for each chunk,
    adjusted to newline boundaries.
    """
    file_size = os.path.getsize(filename)
    chunk_size = file_size // num_chunks
    boundaries = []
    with open(filename, "rb") as f:
        start = 0
        for i in range(num_chunks):
            f.seek(start + chunk_size)
            # Adjust to the end of the current line
            line = f.readline()
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
    
    # Determine file chunk boundaries for multiprocessing
    boundaries = get_chunk_boundaries(input_file, num_processes)
    overall_stats = defaultdict(default_stats)
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [
            executor.submit(process_file_chunk, input_file, start, end, num_threads)
            for (start, end) in boundaries
        ]
        for future in concurrent.futures.as_completed(futures):
            chunk_stats = future.result()
            merge_stats(overall_stats, chunk_stats)
    
    # Write results to output file in sorted order by city
    with open(output_file, "w", encoding="utf-8") as fout:
        for city in sorted(overall_stats.keys()):
            m, M, s, cnt = overall_stats[city]
            avg = s / cnt
            fout.write(f"{city}={round_up(m,1)}/{round_up(avg,1)}/{round_up(M,1)}\n")

if __name__ == "__main__":
    main()
