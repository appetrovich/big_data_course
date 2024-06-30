import struct
import time

def process_file_sequential(filename):
    """
    Подсчет с использованием последовательного чтения
    """
    total_sum = 0
    min_val = None
    max_val = None
    
    with open(filename, 'rb') as f:
        while chunk := f.read(4):
            number = struct.unpack('>I', chunk)[0]
            total_sum += number
            if min_val is None or number < min_val:
                min_val = number
            if max_val is None or number > max_val:
                max_val = number
    
    return total_sum, min_val, max_val

if __name__ == "__main__":
    filename = 'random_integers.bin'
    start = time.time()
    total_sum, min_val, max_val = process_file_sequential(filename)
    end = time.time()
    print(f"Время подсчета: {end - start}")
    print(f"Общая сумма: {total_sum}")
    print(f"Минимальное значение: {min_val}")
    print(f"Максимальное значение: {max_val}")