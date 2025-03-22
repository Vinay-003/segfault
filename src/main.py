import concurrent.futures
import csv
import io
import os
import math
from collections import defaultdict
import mmap
# import time

# Use all available CPU cores
NUM_PROCESSES = os.cpu_count()

def round_up(value, decimals=1):
    """Rounds value upward (toward +∞) to the specified number of decimal places."""
    factor = 10 ** decimals
    return math.ceil(value * factor) / factor

def process_chunk(chunk_bytes):
    """
    Process a chunk (independent bytes object) of the file.
    Uses the CSV module for fast parsing and aggregates scores per city.
    Returns a dictionary mapping each city to a list of scores.
    """
    city_scores = defaultdict(list)
    # Wrap bytes in a BytesIO stream then a TextIOWrapper for proper decoding.
    bio = io.BytesIO(chunk_bytes)
    # The csv.reader is very fast because it's implemented in C.
    reader = csv.reader(io.TextIOWrapper(bio, encoding='utf-8'), delimiter=';')
    for row in reader:
        if len(row) != 2:
            continue  # Skip malformed lines
        city = row[0].strip()
        try:
            score = float(row[1].strip())
        except ValueError:
            continue  # Skip invalid numbers
        city_scores[city].append(score)
    return city_scores

def main(input_file="testcase.txt", output_file="output.txt"):
    city_data = defaultdict(list)
    file_size = os.path.getsize(input_file)
    
    # If file is empty, write empty output and exit.
    if file_size == 0:
        with open(output_file, "w") as f:
            f.write("")
        return

    # Open the file in binary mode and memory-map it.
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
                    # Advance to the end of the current line.
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
    for res in results:
        for city, scores in res.items():
            city_data[city].extend(scores)
    
    # Sort cities alphabetically.
    sorted_cities = sorted(city_data.keys())
    
    # Write aggregated statistics to the output file.
    with open(output_file, "w") as f:
        for city in sorted_cities:
            scores = city_data[city]
            if scores:
                min_score = min(scores)
                mean_score = sum(scores) / len(scores)
                max_score = max(scores)
                f.write(f"{city}={round_up(min_score,1)}/"
                        f"{round_up(mean_score,1)}/"
                        f"{round_up(max_score,1)}\n")

if __name__ == "__main__":
    # start_time = time.perf_counter()
    main()
    # end_time = time.perf_counter()
    # print(f"Execution Time: {end_time - start_time:.4f} seconds")
