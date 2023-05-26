def num_in_range(row, min, max):
    count = 0
    for n in row:
        if min <= n <= max:
            count += 1
    return count


def num_in_range_2(i, row, min, max):
    count = 0
    for n in row:
        if min <= n <= max:
            count += 1
    return (i, count)
