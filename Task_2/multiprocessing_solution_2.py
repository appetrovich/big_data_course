import os
import time
import multiprocessing

from prime_factors import prime_factors_count

def worker(numbers, result_queue, lock):
    total_count = 0
    for number in numbers:
        total_count += prime_factors_count(number)
    with lock:
        result_queue.put(total_count)

def multiprocessing_prime_factors_count(file_path):
    with open(file_path, 'r') as f:
        numbers = [int(line.strip()) for line in f]

    cpu_count = multiprocessing.cpu_count()
    chunk_size = len(numbers) // cpu_count
    chunks = [numbers[i:i + chunk_size] for i in range(0, len(numbers), chunk_size)]

    result_queue = multiprocessing.Queue()
    lock = multiprocessing.Lock()
    processes = []
    for chunk in chunks:
        p = multiprocessing.Process(target=worker, args=(chunk, result_queue, lock))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    total_count = 0
    while not result_queue.empty():
        total_count += result_queue.get()
    return total_count


if __name__ == "__main__":
    filename = 'Task_2/random_integers_2.txt'
    start = time.time()
    result = multiprocessing_prime_factors_count(filename)
    end = time.time()
    print(f"Время подсчета: {end - start}")
    print(f"Общая сумма: {result}")