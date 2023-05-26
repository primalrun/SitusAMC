import time
import multiprocessing as mp


def num_in_range_2(i, row, min, max):
    count = 0
    for n in row:
        if min <= n <= max:
            count += 1
    return (i, count)


if __name__ == '__main__':
    results = []

    def collect_result(result):
        global results
        results.append(result)

    start_time = time.time()

    a = [2, 5, 8]
    # b = [a] * 100000000
    b = [a] * 1000000

    p = mp.Pool(mp.cpu_count())
    for i, row in enumerate(b):
        p.apply_async(num_in_range_2, args=(i, row, 3, 9), callback=collect_result)

    p.close()
    p.join()

    print(len(results))
    print(f'{round(time.time() - start_time, 3)} seconds')

    # 31 seconds without parallelization
