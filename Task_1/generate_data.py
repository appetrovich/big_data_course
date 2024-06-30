import os
import time
import random
import struct

# Генерация бинарных данных
def create_binary_file(filename, size_in_gb):
    num_integers = size_in_gb * (1024**3) // 4 
    with open(filename, 'wb') as f:
        for _ in range(num_integers):
            # Генерация случайного 32-разрядного беззнакового целого числа
            number = random.randint(0, 0xFFFFFFFF)
            # Запись числа в файл в формате big endian
            f.write(struct.pack('>I', number))

if __name__ == "__main__":
    start = time.time()
    create_binary_file('random_integers.bin', 2)  # Создание 2Гб файла
    end = time.time()
    print(f"Время генерации обычным методом в секундах: {end - start}")