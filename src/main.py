import concurrent.futures
import os
from collections import defaultdict
import math

NUM_THREADS = os.cpu_count()  # Use all available cores

def round_up(x, digits=1):
    """
    Rounds x upward (toward +∞) to the specified number of decimal places.
    For example:
      round_up(-0.1500001, 1) returns -0.1
      round_up(2.341, 1) returns 2.4
    """
    factor = 10 ** digits
    return math.ceil(x * factor) / factor

def process_chunk(lines):
    """
    Process a chunk of lines and return a dictionary mapping cities to a list of scores.
    """
    city_scores = defaultdict(list)
    for line in lines:
        parts = line.strip().split(";")
        if len(parts) != 2:
            continue  # Skip malformed lines
        city = parts[0].strip()
        try:
            score = float(parts[1].strip())
            city_scores[city].append(score)
        except ValueError:
            continue  # Skip lines where the score isn't numeric
    return city_scores

def main(input_file="testcase.txt", output_file="output.txt"):
    city_data = defaultdict(list)

    # Read the entire file with a large buffer for efficiency
    with open(input_file, "r", buffering=2**20) as fin:
        lines = fin.readlines()

    # Break the file into chunks for parallel processing
    chunk_size = max(1, len(lines) // NUM_THREADS)
    chunks = [lines[i:i+chunk_size] for i in range(0, len(lines), chunk_size)]

    # Process chunks in parallel using threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        results = executor.map(process_chunk, chunks)

    # Aggregate the results
    for result in results:
        for city, scores in result.items():
            city_data[city].extend(scores)

    # Sort cities alphabetically
    sorted_cities = sorted(city_data.keys())

    # Write results to the output file
    with open(output_file, "w") as fout:
        for city in sorted_cities:
            scores = city_data[city]
            if not scores:
                continue
            min_score = min(scores)
            avg_score = sum(scores) / len(scores)
            max_score = max(scores)
            # Use round_up to round all numbers upward (toward +∞)
            fout.write(f"{city}={round_up(min_score,1)}/"
                       f"{round_up(avg_score,1)}/"
                       f"{round_up(max_score,1)}\n")

if __name__ == "__main__":
    main()
