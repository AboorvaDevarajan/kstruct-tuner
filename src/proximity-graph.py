import pandas as pd
import numpy as np
from multiprocessing import Pool
import argparse

def process_chunk_window(chunk_data, window_size):
    field_pairs = {}
    chunk_data = chunk_data.sort_values('Timestamp')
    timestamps = chunk_data['Timestamp'].values
    fields = chunk_data['Field'].values
    
    for i in range(len(chunk_data)):
        current_time = timestamps[i]
        current_field = fields[i]
        
        j = i - 1
        while j >= 0 and current_time - timestamps[j] <= window_size:
            if fields[j] != current_field:
                pair = tuple(sorted([current_field, fields[j]]))
                field_pairs[pair] = field_pairs.get(pair, 0) + 1
            j -= 1
        
        j = i + 1
        while j < len(chunk_data) and timestamps[j] - current_time <= window_size:
            if fields[j] != current_field:
                pair = tuple(sorted([current_field, fields[j]]))
                field_pairs[pair] = field_pairs.get(pair, 0) + 1
            j += 1
    
    return field_pairs


def process_chunk_stack(chunk_data, stack_distance_threshold):
    field_pairs = {}
    access_stack = []
    
    for _, row in chunk_data.iterrows():
        current_field = row['Field']
        
        if current_field in access_stack:
            access_stack.remove(current_field)
        access_stack.append(current_field)
        
        for distance, stack_field in enumerate(reversed(access_stack[:-1]), start=1):
            if distance > stack_distance_threshold:
                break
            if current_field != stack_field:
                pair = tuple(sorted([current_field, stack_field]))
                field_pairs[pair] = field_pairs.get(pair, 0) + 1
    
    return field_pairs


def merge_field_pairs_incrementally(new_pairs, merged_pairs):
    for pair, count in new_pairs.items():
        merged_pairs[pair] = merged_pairs.get(pair, 0) + count
    return merged_pairs


def create_adjacency_matrix(field_pairs):
    fields = sorted(set([field for pair in field_pairs.keys() for field in pair]))
    adj_matrix = np.zeros((len(fields), len(fields)))
    field_index = {field: idx for idx, field in enumerate(fields)}
    
    for pair, count in field_pairs.items():
        i, j = field_index[pair[0]], field_index[pair[1]]
        adj_matrix[i, j] = count
        adj_matrix[j, i] = count
    
    return pd.DataFrame(adj_matrix, index=fields, columns=fields)


def process_in_chunks(filename, method, param, output_file, n_jobs):
    merged_field_pairs = {}

    chunk_iter = pd.read_csv(filename, chunksize=100000, dtype={'Timestamp': 'float32', 'Field': 'category'})
    
    with Pool(n_jobs) as pool:
        results = []
        for chunk in chunk_iter:
            if method == "window":
                results.append(pool.apply_async(process_chunk_window, (chunk, param)))
            elif method == "stack":
                results.append(pool.apply_async(process_chunk_stack, (chunk, param)))
        
        for result in results:
            new_field_pairs = result.get()
            merged_field_pairs = merge_field_pairs_incrementally(new_field_pairs, merged_field_pairs)
    
    adj_matrix = create_adjacency_matrix(merged_field_pairs)
    adj_matrix.to_csv(output_file)


def main():

    parser = argparse.ArgumentParser(description="Generate adjacency matrix based on method.")

    parser.add_argument("--method", choices=["window", "stack"], required=True,
                        help="Method to use: 'window' or 'stack'.")
    parser.add_argument("--window_size", type=float, default=3e-6, help="Window size for 'window' method.")
    parser.add_argument("--stack_distance_threshold", type=int, default=10, help="Stack distance threshold for 'stack' method.")
    parser.add_argument("--n_jobs", type=int, default=4, help="Number of parallel jobs.")
    parser.add_argument("--input_file", type=str, required=True, help="Input CSV file.")
    parser.add_argument("--output_file", type=str, required=True, help="Output CSV file.")

    args = parser.parse_args()
    
    if args.method == "window":
        process_in_chunks(args.input_file, "window", args.window_size, args.output_file, args.n_jobs)
    elif args.method == "stack":
        process_in_chunks(args.input_file, "stack", args.stack_distance_threshold, args.output_file, args.n_jobs)

if __name__ == "__main__":
    main()

