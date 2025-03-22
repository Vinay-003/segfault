import math
import mmap
import multiprocessing
import os 
from collections import defaultdict

def round_inf(x):
    """Rounds x upward (toward +∞) to one decimal place."""
    return math.ceil(x * 10) / 10  

def default_city_data():
    # [min, max, sum, count]
    return [float('inf'), float('-inf'), 0.0, 0]

def process_chunk(filename, start_offset, end_offset):
    data = defaultdict(default_city_data)
    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        size = len(mm)
        
        # Adjust start_offset to next newline if not at beginning.
        if start_offset != 0:
            while start_offset < size and mm[start_offset] != ord('\n'):
                start_offset += 1
            start_offset += 1  # skip newline
        
        # Adjust end_offset to end of current line.
        end = end_offset
        while end < size and mm[end] != ord('\n'):
            end += 1
        if end < size:
            end += 1  # include newline
        
        chunk = mm[start_offset:end]
        mm.close()
    
    # Process each line in the chunk.
    for line in chunk.split(b'\n'):
        if not line:
            continue
        
        semicolon_pos = line.find(b';')
        if semicolon_pos == -1:
            continue
        
        city = line[:semicolon_pos]  
        score_str = line[semicolon_pos+1:]
        
        try:
            score = float(score_str)
        except ValueError:
            continue
        
        entry = data[city]
        entry[0] = min(entry[0], score)
        entry[1] = max(entry[1], score)
        entry[2] += score
        entry[3] += 1
    
    return data

def merge_data(data_list):
    final = defaultdict(default_city_data)  
    for data in data_list:
        for city, stats in data.items():
            final_entry = final[city]
            final_entry[0] = min(final_entry[0], stats[0])
            final_entry[1] = max(final_entry[1], stats[1])
            final_entry[2] += stats[2]
            final_entry[3] += stats[3]
    return final

def get_chunk_boundaries(filename, num_chunks):
    file_size = os.path.getsize(filename)
    chunk_size = file_size // num_chunks
    boundaries = []
    with open(filename, "rb") as f:
        start = 0
        for i in range(num_chunks):
            f.seek(start + chunk_size)
            line = f.readline()  # move to end of current line
            if not line:
                end = file_size
            else:
                end = f.tell()
            boundaries.append((start, end))
            start = end
        if boundaries:
            boundaries[-1] = (boundaries[-1][0], file_size)
    return boundaries

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    # You can adjust num_procs. For a 2-core server, using 2 or 3 might be optimal.
    num_procs = max(1, multiprocessing.cpu_count() - 1)
    chunk_size = os.path.getsize(input_file_name) // num_procs
    chunks = get_chunk_boundaries(input_file_name, num_procs)
    
    with multiprocessing.Pool(num_procs) as pool:
        tasks = [(input_file_name, start, end) for start, end in chunks]
        results = pool.starmap(process_chunk, tasks)
    
    final_data = merge_data(results)
    
    out_lines = []
    for city in sorted(final_data.keys()):
        mn, mx, total, count = final_data[city]
        avg = total / count
        # Decode city (it is in bytes) using UTF-8; replace errors if needed.
        out_lines.append(f"{city.decode('utf-8', errors='replace')}={round_inf(mn):.1f}/{round_inf(avg):.1f}/{round_inf(mx):.1f}\n")
    
    with open(output_file_name, "w") as f:
        f.writelines(out_lines)

if __name__ == "__main__":
    main()
