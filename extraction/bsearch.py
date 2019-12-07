import pickle

from tqdm import tqdm


class BinarySearchFile:
    def __init__(self, file_name, index=None, mode='r', encoding=None):
        self.file_name = file_name
        self.f = open(file_name, mode=mode, encoding=encoding)
        if index is None:
            self.index = self._create_byte_index()
        elif isinstance(index, str):
            self.index = self._unpickle_index(index)
        else:
            self.index = index
        self.n_lines = len(self.index)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.f.close()

    def __getitem__(self, line_num):
        return self.get_line_key_value(line_num)

    def _create_byte_index(self):
        locs = [0]
        with open(self.file_name, 'rb') as f:
            for _ in tqdm(f, desc='Indexing file', unit_scale=True):
                locs.append(f.tell())
        return locs[:-1]

    def _unpickle_index(self, filepath):
        with open(filepath, 'rb') as f:
            return pickle.load(f)

    def pickle_index(self, filepath):
        with open(filepath, 'wb') as f:
            pickle.dump(self.index, f)

    def get_line_key_value(self, line_num):
        self.f.seek(self.index[line_num])
        line = self.f.readline()
        # TODO: Write a proper key-value pair tool
        i = line.rfind(',')
        return line[:i], line[i + 1:-1]

    def _binary_search(self, key, low, high):
        if low > high:
            return False, -1

        middle = (low + high) // 2
        result, value = self.get_line_key_value(middle)

        if result == key:
            return value, middle

        if result > key:
            return self._binary_search(key, low, middle - 1)
        else:
            return self._binary_search(key, middle + 1, high)

    def search(self, key):
        return self._binary_search(key, 0, self.n_lines - 1)

    def search_many(self, keys, low=None, high=None):
        if low is None:
            low = 0
        if high is None:
            high = self.n_lines - 1

        results = [(False, -1)] * len(keys)

        # Finds an upper bound
        for j in range(len(keys) - 1, -1, -1):
            value, i = self._binary_search(keys[j], low, high)
            results[j] = (value, i)
            if i > -1:
                high = i
                break

        for k in range(j):
            value, i = self._binary_search(keys[k], low, high)
            results[k] = (value, i)
            if i > -1:
                low = i

        return results, high


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
