import reader
import datetime

if __name__ == '__main__':
    print reader.get_indices('hello_', start=datetime.date(2018, 3, 19), end=datetime.date(2018, 3, 23))