from collections import defaultdict
import math

def round_to_infinity(x, digits=1):
    """Rounds x upward (toward +∞) to 1 decimal place."""
    factor = 10 ** digits
    return math.ceil(x * factor) / factor

def process_file(input_file_name="testcase.txt", output_file_name="output.txt"):
    # Aggregates: {city: {'min': float, 'sum': float, 'count': int, 'max': float}}
    city_aggregates = defaultdict(lambda: {'min': float('inf'), 'sum': 0, 'count': 0, 'max': float('-inf')})
    
    # Stream file reading and processing
    with open(input_file_name, "r", buffering=16384) as f:  # 16KB buffer
        for line in f:
            try:
                city, score_str = line.strip().split(";", 1)  # Split once only
                score = float(score_str)
                agg = city_aggregates[city]
                agg['min'] = min(agg['min'], score)
                agg['sum'] += score
                agg['count'] += 1
                agg['max'] = max(agg['max'], score)
            except (ValueError, IndexError):
                continue  # Skip malformed lines or invalid floats

    # Write results
    with open(output_file_name, "w") as out:
        for city in sorted(city_aggregates.keys()):
            agg = city_aggregates[city]
            if agg['count'] == 0:
                continue
            mean = agg['sum'] / agg['count']
            out.write(f"{city}={round_to_infinity(agg['min'], 1)}/"
                      f"{round_to_infinity(mean, 1)}/"
                      f"{round_to_infinity(agg['max'], 1)}\n")

if __name__ == "__main__":
    process_file()