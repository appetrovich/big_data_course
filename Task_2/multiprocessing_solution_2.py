import os
import time
import multiprocessing

from prime_factors import prime_factors_count

# Функция подсчета с использованием multiprocessing
def worker(numbers):
    total_count = 0
    for number in numbers:
        total_count += prime_factors_count(number)
    return total_count

def multiprocessing_prime_factors_count(file_path):
    with open(file_path, 'r') as f:
        numbers = [int(line.strip()) for line in f]

    cpu_count = os.cpu_count()
    chunk_size = len(numbers) // cpu_count
    chunks = [numbers[i:i + chunk_size] for i in range(0, len(numbers), chunk_size)]

    with multiprocessing.Pool(cpu_count) as pool:
        results = pool.map(worker, chunks)
    return sum(results)


if __name__ == "__main__":
    filename = 'Task_2/random_integers_2.txt'
    start = time.time()
    result = multiprocessing_prime_factors_count(filename)
    end = time.time()
    print(f"Время подсчета: {end - start}")
    print(f"Общая сумма: {result}")