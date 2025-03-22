import os
import math
import mmap
import multiprocessing
from collections import defaultdict

# Function to round values up to the nearest tenth
def ceil_to_tenth(value):
    return math.ceil(value * 10) / 10  

# Initializes default city statistics
def init_city_stats():
    return [float('inf'), float('-inf'), 0.0, 0]

# Processes a file segment and extracts statistics
def analyze_segment(file_path, start, end):
    city_stats = defaultdict(init_city_stats)

    with open(file_path, "rb") as file:
        mmapped_file = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        total_size = len(mmapped_file)

        # Ensure the segment starts at a full line
        if start != 0:
            while start < total_size and mmapped_file[start] != ord('\n'):
                start += 1
            start += 1

        # Ensure the segment ends at a full line
        while end < total_size and mmapped_file[end] != ord('\n'):
            end += 1
        if end < total_size:
            end += 1

        data_chunk = mmapped_file[start:end]
        mmapped_file.close()

    # Process each line in the chunk
    for entry in data_chunk.split(b'\n'):
        if not entry:
            continue
        
        separator = entry.find(b';')
        if separator == -1:
            continue
        
        city_name = entry[:separator]
        score_data = entry[separator + 1:]

        try:
            score = float(score_data)
        except ValueError:
            continue

        record = city_stats[city_name]
        record[0] = min(record[0], score)
        record[1] = max(record[1], score)
        record[2] += score
        record[3] += 1

    return city_stats

# Combines results from multiple processes
def combine_results(result_list):
    final_stats = defaultdict(init_city_stats)

    for segment_data in result_list:
        for city, values in segment_data.items():
            merged_record = final_stats[city]
            merged_record[0] = min(merged_record[0], values[0])
            merged_record[1] = max(merged_record[1], values[1])
            merged_record[2] += values[2]
            merged_record[3] += values[3]

    return final_stats

# Main execution function
def execute_analysis(input_file="testcase.txt", output_file="output.txt"):
    # Determine system resources
    core_count = os.cpu_count() or 4  # Default to 4 if detection fails
    process_count = core_count  
    thread_factor = 2  # Each process should ideally handle multiple threads

    # Determine file size
    with open(input_file, "rb") as file:
        mmapped_file = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        total_bytes = len(mmapped_file)
        mmapped_file.close()

    # Divide file into segments
    segment_size = total_bytes // process_count
    partitions = [(i * segment_size, (i + 1) * segment_size if i < process_count - 1 else total_bytes)
                  for i in range(process_count)]

    # Process segments in parallel
    with multiprocessing.Pool(process_count) as worker_pool:
        job_list = [(input_file, start, end) for start, end in partitions]
        partial_results = worker_pool.starmap(analyze_segment, job_list)

    # Merge results
    final_data = combine_results(partial_results)

    # Format output
    output_lines = []
    for city in sorted(final_data.keys()):
        min_val, max_val, total_sum, count = final_data[city]
        avg_val = ceil_to_tenth(total_sum / count)
        output_lines.append(f"{city.decode()}={ceil_to_tenth(min_val):.1f}/{avg_val:.1f}/{ceil_to_tenth(max_val):.1f}\n")

    # Save results
    with open(output_file, "w") as file:
        file.writelines(output_lines)

if __name__ == "__main__":
    execute_analysis()
