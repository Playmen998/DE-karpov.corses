import pandas as pd
import os
import csv
from datetime import datetime

def handler_file(list):
    data_filter = [list[1], list[9], list[-5]] #month, type, amount
    date = datetime.strptime(data_filter[0],"%Y-%m-%d %H:%M:%S").date()
    if int(date.year) != 2020:
        return None
    if data_filter[1] == '':
        return None
    data_filter[0] = f'{date.year}-{date.month}'
    return data_filter




def perform_map(directory, output_file):
    with open(output_file, 'w', encoding='utf-8') as out_file:
        for filename in os.listdir(directory):
            if filename.endswith(".csv"):
                filepath = os.path.join(directory, filename)
                with open(filepath, 'r', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    start = 0
                    data = []
                    for row in reader:
                        if start == 0:
                            start += 1
                            continue
                        data.append(handler_file(row))
                    cleare_data = [item for item in data if item is not None]
                    with open(output_file, 'a') as file:
                        for sublist in cleare_data:
                            file.write(' '.join(map(str, sublist)) + '\n')


if __name__ == '__main__':
    directory = "Data"
    output_file = "output.txt"
    perform_map(directory, output_file)