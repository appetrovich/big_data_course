import os
import time
import mmap
import struct

from concurrent.futures import ThreadPoolExecutor

def analyze_chunk(file_name, start, size):
    total_sum = 0
    min_val = None
    max_val = None

    with open(file_name, 'rb') as f:
        with mmap.mmap(f.fileno(), length=size, offset=start, access=mmap.ACCESS_READ) as mm:
            for i in range(0, size, 4):
                number = struct.unpack('>I', mm[i:i+4])[0]
                total_sum += number
                if min_val is None or number < min_val:
                    min_val = number
                if max_val is None or number > max_val:
                    max_val = number
    
    return total_sum, min_val, max_val

def process_file_threading(file_name, threads_count):
    file_size = os.path.getsize(file_name)
    chunk_size = file_size // threads_count

    results = []
    with ThreadPoolExecutor(max_workers=threads_count) as executor:
        futures = []
        for i in range(threads_count):
            start = i * chunk_size
            size = chunk_size if i < threads_count - 1 else file_size - start
            futures.append(executor.submit(analyze_chunk, file_name, start, size))
        
        for future in futures:
            results.append(future.result())

    total_sum = sum(result[0] for result in results)
    min_num = min(result[1] for result in results)
    max_num = max(result[2] for result in results)
    
    return total_sum, min_num, max_num

if __name__ == "__main__":
    filename = 'random_integers.bin'
    threads_count = 4

    start = time.time()
    total_sum, min_val, max_val = process_file_threading(filename, threads_count)
    end = time.time()
    print(f"Время подсчета: {end - start}")
    print(f"Общая сумма: {total_sum}")
    print(f"Минимальное значение: {min_val}")
    print(f"Максимальное значение: {max_val}")