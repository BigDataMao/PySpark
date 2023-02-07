import os

path = os.getcwd()
print(path)
with open(r'./words.txt', 'r', encoding='utf8') as f:
    str1 = f.readline()
    while str1 != '':
        print(str1, end='')
        str1 = f.readline()
