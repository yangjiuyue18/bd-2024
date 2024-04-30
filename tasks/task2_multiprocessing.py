from multiprocessing import Pool
import time

def calculate(num):
    factors = []
    i = 2
    while i * i <= num:
        if num % i == 0:
            factors.append(i)
            num = num // i
        else:
            i += 1
    if num > 1:
        factors.append(num)
    return len(factors) 

if __name__ == "__main__":
    with open('tasks/numbers.txt', 'r') as file:
        numbers = [int(line.strip()) for line in file.readlines()]

    start_time = time.time() 

    # 使用multiprocessing.Pool来处理任务
    # Используйте multiprocessing.Pool для обработки задач
    with Pool() as p:
        results = p.map(calculate, numbers)

    end_time = time.time() 

    print(f"Total prime factors: {sum(results)}")
    print(f"Processing time: {end_time - start_time:.2f} seconds")