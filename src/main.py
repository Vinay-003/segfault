import concurrent.futures
from collections import defaultdict
import math
import os
import mmap

# Use all available cores
NUM_WORKERS = os.cpu_count() or 1

def round_to_infinity(x, digits=1):
    """
    Rounds x upward (toward +∞) to the specified number of decimal places.
    For example:
      round_to_infinity(-0.1500001, 1) returns -0.1
      round_to_infinity(2.341, 1) returns 2.4
    """
    factor = 10 ** digits
    return math.ceil(x * factor) / factor

def process_chunk(lines):
    """
    Process a chunk (list of lines) and return a dictionary mapping cities to a list of scores.
    This function is run in a separate process.
    """
    city_scores = defaultdict(list)
    for line in lines:
        # Use split with maxsplit=1 for a small speedup.
        parts = line.strip().split(";", 1)
        if len(parts) != 2:
            continue
        city, score_str = parts[0].strip(), parts[1].strip()
        try:
            score = float(score_str)
            city_scores[city].append(score)
        except ValueError:
            continue
    return city_scores

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    city_data = defaultdict(list)

    # Use memory mapping to quickly load the file content.
    with open(input_file_name, "r") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            # Decode once and split into lines.
            lines = mm.read().decode('utf-8').splitlines()

    # Split the list of lines into chunks (each chunk gets roughly equal lines).
    chunk_size = max(1, len(lines) // NUM_WORKERS)
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    # Use ProcessPoolExecutor for CPU-bound parsing.
    with concurrent.futures.ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        results = executor.map(process_chunk, chunks)

    # Aggregate results from each worker.
    for partial in results:
        for city, scores in partial.items():
            city_data[city].extend(scores)

    # Sort cities alphabetically.
    sorted_cities = sorted(city_data.keys())

    with open(output_file_name, "w") as output_file:
        for city in sorted_cities:
            if not city_data[city]:
                continue
            min_score = min(city_data[city])
            mean_score = sum(city_data[city]) / len(city_data[city])
            max_score = max(city_data[city])
            output_file.write(f"{city}={round_to_infinity(min_score, 1)}/"
                              f"{round_to_infinity(mean_score, 1)}/"
                              f"{round_to_infinity(max_score, 1)}\n")

if __name__ == "__main__":
    main()
