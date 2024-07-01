import time

from prime_factors import prime_factors_count

# Функция подсчета последовательным алгоритмом
def sequential_prime_factors_count(file_path):
    total_count = 0
    with open(file_path, 'r') as f:
        for line in f:
            number = int(line.strip())
            total_count += prime_factors_count(number)
    return total_count

if __name__ == "__main__":
    filename = 'Task_2/random_integers_2.txt'
    start = time.time()
    result = sequential_prime_factors_count(filename)
    end = time.time()
    print(f"Время подсчета: {end - start}")
    print(f"Общая сумма: {result}")