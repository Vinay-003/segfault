import math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

def round_to_infinity(x, d=1):
    factor = 10 ** d
    # For non-negative values, round up; for negatives, truncate (per provided behavior)
    return (math.ceil if x >= 0 else math.trunc)(x * factor) / factor

def process_chunk(lines):
    # Create a local dict: city -> [min, max, total, count]
    local = defaultdict(lambda: [float('inf'), float('-inf'), 0.0, 0])
    for line in lines:
        parts = line.split(';')
        if len(parts) != 2:
            continue
        city, score = parts[0].strip(), parts[1].strip()
        try:
            s = float(score)
        except ValueError:
            continue
        stats = local[city]
        stats[0] = s if s < stats[0] else stats[0]
        stats[1] = s if s > stats[1] else stats[1]
        stats[2] += s
        stats[3] += 1
    return local

def merge_dicts(dicts):
    merged = defaultdict(lambda: [float('inf'), float('-inf'), 0.0, 0])
    for d in dicts:
        for city, stats in d.items():
            mstats = merged[city]
            mstats[0] = stats[0] if stats[0] < mstats[0] else mstats[0]
            mstats[1] = stats[1] if stats[1] > mstats[1] else mstats[1]
            mstats[2] += stats[2]
            mstats[3] += stats[3]
    return merged

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    with open(input_file_name, "r") as f:
        lines = f.read().splitlines()

    # Use up to 8 threads (or fewer if there are fewer lines)
    num_threads = min(8, len(lines))
    # Create chunks (each chunk gets roughly equal number of lines)
    chunk_size = (len(lines) + num_threads - 1) // num_threads
    chunks = [lines[i*chunk_size:(i+1)*chunk_size] for i in range(num_threads)]
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = list(executor.map(process_chunk, chunks))
    
    final_data = merge_dicts(results)
    
    with open(output_file_name, "w") as f:
        for city in sorted(final_data):
            mini, maxi, total, count = final_data[city]
            avg = round_to_infinity(total / count)
            f.write(f"{city}={round_to_infinity(mini):.1f}/{avg:.1f}/{round_to_infinity(maxi):.1f}\n")

if __name__ == "__main__":
    main()
