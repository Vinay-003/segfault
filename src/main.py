import concurrent.futures
import os
import math
from collections import defaultdict
import mmap
import time

NUM_THREADS = os.cpu_count()  # Dynamically determine number of threads

def round_up(value, decimals=1):
    """Rounds value upward (toward +∞) to the specified number of decimal places."""
    factor = 10 ** decimals
    return math.ceil(value * factor) / factor

def process_chunk(chunk_bytes):
    """
    Process a chunk (a bytes object) of the file.
    Splits the chunk into lines, decodes, parses, and aggregates scores per city.
    Returns a dictionary mapping each city to a list of scores.
    """
    city_scores = defaultdict(list)
    for line in chunk_bytes.split(b'\n'):
        if not line:
            continue
        try:
            parts = line.decode('utf-8').strip().split(';')
        except UnicodeDecodeError:
            continue  # Skip lines that cannot be decoded
        if len(parts) != 2:
            continue  # Skip malformed lines
        city = parts[0].strip()
        try:
            score = float(parts[1].strip())
        except ValueError:
            continue  # Skip lines with invalid numbers
        city_scores[city].append(score)
    return city_scores

def main(input_file="testcase.txt", output_file="output.txt"):
    city_data = defaultdict(list)
    file_size = os.path.getsize(input_file)
    
    # If the file is empty, create an empty output and exit.
    if file_size == 0:
        print("Input file is empty. No processing done.")
        with open(output_file, "w") as f:
            f.write("")
        return

    # Open file in binary mode and create an mmap.
    with open(input_file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            # We'll split the file into NUM_THREADS chunks, aligning to newline boundaries.
            chunks = []
            offset = 0
            # Estimate an initial chunk size.
            approx_chunk_size = file_size // NUM_THREADS

            for i in range(NUM_THREADS):
                if offset >= file_size:
                    break
                # For the last chunk, take the rest of the file.
                if i == NUM_THREADS - 1:
                    end = file_size
                else:
                    end = offset + approx_chunk_size
                    # Advance end until after the next newline (if not already at one)
                    if end < file_size:
                        while end < file_size and mm[end] != ord('\n'):
                            end += 1
                        # Include the newline character in the chunk.
                        if end < file_size:
                            end += 1
                chunk = mm[offset:end]
                # Convert the memoryview slice to a new independent bytes object.
                chunks.append(bytes(chunk))
                offset = end
        finally:
            mm.close()

    # Process chunks in parallel using a ThreadPoolExecutor.
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        results = executor.map(process_chunk, chunks)
    
    # Aggregate results from all threads.
    for res in results:
        for city, scores in res.items():
            city_data[city].extend(scores)

    # Sort cities alphabetically.
    sorted_cities = sorted(city_data.keys())
    
    # Write the output with min/mean/max for each city (rounded upward to one decimal).
    with open(output_file, "w") as f:
        for city in sorted_cities:
            scores = city_data[city]
            if not scores:
                continue
            min_score = min(scores)
            mean_score = sum(scores) / len(scores)
            max_score = max(scores)
            f.write(f"{city}={round_up(min_score, 1)}/"
                    f"{round_up(mean_score, 1)}/"
                    f"{round_up(max_score, 1)}\n")

if __name__ == "__main__":
    # start_time = time.perf_counter()
    main()
    # end_time = time.perf_counter()
    # print(f"Execution Time: {end_time - start_time:.4f} seconds")
