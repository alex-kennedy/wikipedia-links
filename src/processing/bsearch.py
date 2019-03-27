import os
import time

class BinarySearchFile:
    def __init__(self, file_name, num_lines, line_len):
        self.file_name = file_name
        self.num_lines = num_lines
        self.line_len = line_len

        self.f = open(file_name, 'rb')
        self.size = os.stat(file_name).st_size

    def __getitem__(self, line_num):
        self.f.seek(self.line_len * line_num)
        return self.f.readline()

    def search(self, key):
        return self.binary_search(key, 0, self.num_lines - 1)

    def binary_search(self, key, low, high):
        if low > high:
            return -1

        middle = (low + high) // 2

        if self[middle] == key:
            return middle

        if self[middle] > key:
            return self.binary_search(key, low, middle - 1)
        else:
            return self.binary_search(key, middle + 1, high)


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