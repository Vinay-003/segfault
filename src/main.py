import concurrent.futures
import csv
import io
import os
import math
from collections import defaultdict
import mmap

# Use all available CPU cores
NUM_PROCESSES = os.cpu_count()

def round_up(value, decimals=1):
    """Rounds value upward (toward +∞) to the specified number of decimal places."""
    factor = 10 ** decimals
    return math.ceil(value * factor) / factor

def default_stats():
    """Return default stats [min, max, sum, count] for a city."""
    return [float('inf'), float('-inf'), 0.0, 0]

def process_chunk(chunk_bytes):
    """
    Process a chunk (independent bytes object) of the file.
    Uses the CSV module for fast parsing and aggregates scores per city.
    Returns a dictionary mapping each city to [min, max, sum, count].
    """
    city_stats = defaultdict(default_stats)

    # Wrap bytes in a BytesIO stream for CSV processing
    bio = io.BytesIO(chunk_bytes)
    reader = csv.reader(io.TextIOWrapper(bio, encoding='utf-8'), delimiter=';')

    for row in reader:
        if len(row) != 2:
            continue  # Skip malformed lines
        city = row[0]
        try:
            score = float(row[1])
        except ValueError:
            continue  # Skip invalid numbers

        stats = city_stats[city]
        stats[0] = min(stats[0], score)  # min score
        stats[1] = max(stats[1], score)  # max score
        stats[2] += score               # sum of scores
        stats[3] += 1                   # count

    return city_stats

def merge_results(results):
    """Merge multiple dictionaries into a single aggregated dictionary."""
    merged_stats = defaultdict(default_stats)

    for city_data in results:
        for city, stats in city_data.items():
            entry = merged_stats[city]
            entry[0] = min(entry[0], stats[0])
            entry[1] = max(entry[1], stats[1])
            entry[2] += stats[2]
            entry[3] += stats[3]

    return merged_stats

def main(input_file="testcase.txt", output_file="output.txt"):
    file_size = os.path.getsize(input_file)
    
    # If file is empty, write empty output and exit.
    if file_size == 0:
        with open(output_file, "w") as f:
            f.write("")
        return

    # Memory-map the file for efficient reading.
    with open(input_file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            chunks = []
            approx_chunk_size = file_size // NUM_PROCESSES
            offset = 0

            # Split the file into NUM_PROCESSES chunks, aligning on newline boundaries.
            for i in range(NUM_PROCESSES):
                if offset >= file_size:
                    break
                if i == NUM_PROCESSES - 1:
                    end = file_size
                else:
                    end = offset + approx_chunk_size
                    while end < file_size and mm[end] != ord('\n'):
                        end += 1
                    if end < file_size:
                        end += 1  # Include newline

                # Create an independent bytes object for this chunk.
                chunks.append(bytes(mm[offset:end]))
                offset = end
        finally:
            mm.close()

    # Process chunks in parallel using ProcessPoolExecutor.
    with concurrent.futures.ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        results = executor.map(process_chunk, chunks)

    # Merge results from all processes.
    final_stats = merge_results(results)

    # Sort cities alphabetically and write aggregated statistics to the output file.
    with open(output_file, "w") as f:
        for city in sorted(final_stats.keys()):
            min_score, max_score, total_score, count = final_stats[city]
            if count:
                avg_score = round_up(total_score / count, 1)
                f.write(f"{city}={round_up(min_score,1):.1f}/"
                        f"{avg_score:.1f}/"
                        f"{round_up(max_score,1):.1f}\n")

if __name__ == "__main__":
    main()
