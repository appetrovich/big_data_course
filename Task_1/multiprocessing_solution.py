import time
import mmap
import struct
import multiprocessing

def worker(data_chunk, result_queue):
    total_sum = 0
    min_val = None
    max_val = None
    
    for i in range(0, len(data_chunk), 4):
        number = struct.unpack('>I', data_chunk[i:i+4])[0]
        total_sum += number
        if min_val is None or number < min_val:
            min_val = number
        if max_val is None or number > max_val:
            max_val = number
    
    result_queue.put((total_sum, min_val, max_val))

def process_file_multiprocessed(filename, num_workers=4):
    with open(filename, 'rb') as f:
        mmapped_file = mmap.mmap(f.fileno(), offset=0, length=0, access=mmap.ACCESS_READ)
        file_size = mmapped_file.size()
        chunk_size = file_size // num_workers # определяем размер чанка данных

        processes = []
        result_queue = multiprocessing.Queue()

        for i in range(num_workers):
            start = i * chunk_size
            end = start + chunk_size if i < num_workers - 1 else file_size
            data_chunk = mmapped_file[start:end]
            p = multiprocessing.Process(target=worker, args=(data_chunk, result_queue))
            processes.append(p)
            p.start()

        total_sum = 0
        min_val = None
        max_val = None

        for _ in range(num_workers):
            chunk_sum, chunk_min, chunk_max = result_queue.get()
            total_sum += chunk_sum
            if min_val is None or (chunk_min is not None and chunk_min < min_val):
                min_val = chunk_min
            if max_val is None or (chunk_max is not None and chunk_max > max_val):
                max_val = chunk_max

        for p in processes:
            p.join()

    return total_sum, min_val, max_val

if __name__ == "__main__":
    filename = 'random_integers.bin'
    start = time.time()
    total_sum, min_val, max_val = process_file_multiprocessed(filename)
    end = time.time()
    print(f"Время подсчета: {end - start}")
    print(f"Общая сумма: {total_sum}")
    print(f"Минимальное значение: {min_val}")
    print(f"Максимальное значение: {max_val}")