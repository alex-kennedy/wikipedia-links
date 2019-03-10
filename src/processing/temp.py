import string
from random import choice

n = 4
k = 100000

with open('data/raw/unsorted.txt', 'w') as f:
    for _ in range(k):
        s = ''.join(choice(string.ascii_lowercase) for _ in range(n)) + '\n'
        f.write(s)