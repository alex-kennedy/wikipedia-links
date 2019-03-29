import os


class BinarySearchFile:
    def __init__(self,
                 file_name,
                 create_index=True,
                 n_lines=None,
                 line_len=None,
                 key_len=None):
        self.file_name = file_name
        self.f = open(file_name, 'rb')

        if create_index:
            self.index = self._create_byte_index()
            self.n_lines = len(self.index)
            self.get_line = self._index_get
        else:
            if not (n_lines and line_len and key_len):
                raise ValueError('If create_index is not True, n_lines, '
                                 'line_len, and key_len must be provided.')
            self.n_lines = n_lines
            self.line_len = line_len
            self.key_len = key_len
            self.get_line = self._fixed_get

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.f.close()

    def __getitem__(self, line_num):
        return self.get_line(line_num)

    def _create_byte_index(self):
        locs = [0]
        with open(self.file_name, 'rb') as f:
            for _ in f:
                locs.append(f.tell())
        return locs[:-1]

    def _index_get(self, line_num):
        self.f.seek(self.index[line_num])
        line = self.f.readline()
        i = line.rfind(b',')
        return line[:i], line[i + 1:-1]

    def _fixed_get(self, line_num):
        self.f.seek(self.line_len * line_num)
        line = self.f.readline()
        return line[:self.key_len], line[self.key_len:]

    def _binary_search(self, key, low, high):
        if low > high:
            return -1

        middle = (low + high) // 2
        result, value = self.get_line(middle)

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
