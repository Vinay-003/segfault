import concurrent.futures
import os 
from collections import defaultdict
import math

NUM_THREADS = os.cpu_count()   # Use 12 threads for your 12-core processor

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
    """Process a chunk of lines and return a dictionary mapping cities to a list of scores."""
    city_scores = defaultdict(list)
    for line in lines:
        parts = line.strip().split(";")  # Split using ";"
        if len(parts) != 2:
            continue  # Skip malformed lines
        city, score = parts[0].strip(), parts[1].strip()
        try:
            score = float(score)  # Convert score to float
            city_scores[city].append(score)
        except ValueError:
            continue  # Skip invalid numeric values
    return city_scores

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    city_data = defaultdict(list)  # Dictionary to store scores grouped by city

    # Read all lines from the input file using a large buffer for efficiency
    with open(input_file_name, "r", buffering=2**20) as input_file:
        lines = input_file.readlines()

    # Break the file into chunks for parallel processing
    chunk_size = max(1, len(lines) // NUM_THREADS)
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    # Process each chunk concurrently using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        results = executor.map(process_chunk, chunks)

    # Aggregate results from all threads
    for city_scores in results:
        for city, scores in city_scores.items():
            city_data[city].extend(scores)

    # Sort cities alphabetically
    sorted_cities = sorted(city_data.keys())

    # Write results to the output file using the custom rounding function
    with open(output_file_name, "w") as output_file:
        for city in sorted_cities:
            if not city_data[city]:
                continue  # Skip if no valid data for this city
            min_score = min(city_data[city])
            mean_score = sum(city_data[city]) / len(city_data[city])
            max_score = max(city_data[city])
            output_file.write(f"{city}={round_to_infinity(min_score, 1)}/"
                              f"{round_to_infinity(mean_score, 1)}/"
                              f"{round_to_infinity(max_score, 1)}\n")

if __name__ == "__main__":
    main()
