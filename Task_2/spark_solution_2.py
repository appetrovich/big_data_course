import os
import sys
import time

from pyspark import SparkContext

from prime_factors import prime_factors_count

def spark_prime_factors_count(file_path):
    sc = SparkContext(master='local[*]')
    numbers = sc.textFile(file_path).map(lambda x: int(x.strip()))
    counts = numbers.map(prime_factors_count).reduce(lambda x, y: x + y)
    sc.stop()
    return counts

if __name__ == "__main__":
    filename = 'Task_2/random_integers_2.txt'
    start = time.time()
    result = spark_prime_factors_count(filename)
    end = time.time()
    print(f"Время подсчета: {end - start}")
    print(f"Общая сумма: {result}")