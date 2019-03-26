import os

class BinarySearchFile:
    def __init__(self, file_name, num_lines, line_len):
        self.file_name = file_name
        self.num_lines = num_lines
        self.line_len = line_len

        self.f = open(file_name, 'rb')
        self.size = os.stat(file_name).st_size

        for line in self.f:
            print(len(line))

    def __getitem__(self, line_num):
        return

    def search(self, key):
        return

    def binary_search(self, key, low, high):
        if low > high:
            return -1

        middle = (low + high) / 2

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
        for _ in range(564):
            string = ''.join([choice(alphabet) for _ in range(57)])
            strings.append(string)

        for string in sorted(strings):
            out.write(string + '\n')

if __name__ == '__main__':
    # temp()
    bsearch = BinarySearchFile('src/processing/sorted_file.txt', 564, 58)
    print(bsearch.size)