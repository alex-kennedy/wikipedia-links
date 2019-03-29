import os
import time

class BinarySearchFile:
    def __init__(self, file_name, n_lines, line_len, key_len):
        self.file_name = file_name
        self.n_lines = n_lines
        self.line_len = line_len
        self.key_len = key_len

        self.f = open(file_name, 'rb')
        self.size = os.stat(file_name).st_size

    def __getitem__(self, line_num):
        self.f.seek(self.line_len * line_num)
        line = self.f.readline()
        print(line[self.key_len:].strip())
        return line[:self.key_len], line[self.key_len:]

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.f.close()

    def _binary_search(self, key, low, high):
        if low > high:
            return -1

        middle = (low + high) // 2
        result, value = self[middle]

        if result == key:
            return value

        if result > key:
            return self._binary_search(key, low, middle - 1)
        else:
            return self._binary_search(key, middle + 1, high)

    def search(self, key):
        return self._binary_search(key, 0, self.n_lines - 1)

    def search_many(self, keys):
        keys.sort()
        results = []

        low = 0
        for key in keys:
            i = self._binary_search(key, low, self.n_lines - 1)
            if i != -1:
                low = i
            results.append(i)


def temp():
    from random import choice
    alphabet = 'abcdefghijklmnopqrstuvwxyz'
    strings = []
    with open('src/processing/sorted_file.txt', 'w') as out:
        for _ in range(1000000):
            string = ''.join([choice(alphabet) for _ in range(57)])
            strings.append(string)

        for string in sorted(strings):
            out.write(string + '\n')

if __name__ == '__main__':
    # temp()
    # with open('src/processing/sorted_file.txt') as f:
    #     f.seek(58)
    #     print(f.read())

    bsearch = BinarySearchFile('src/processing/sorted_file.txt', 1000000, 58)
    
    start = time.time()
    for i in range(100000):
        bsearch.search(b'ptgikejzsanybrpjoqeuqzknjedcqyaqfzoddcdmdytnlpsrgppwgtklp\n')
    print(time.time() - start)