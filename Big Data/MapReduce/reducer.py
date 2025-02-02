import pandas as pd
import os
import csv
from datetime import datetime



def reducer_map(input_file):
    with open(input_file, 'r') as file:
        lines = file.readlines()

    # Определение словаря для типов платежей
    mapping = {
        1: 'Credit card',
        2: 'Cash',
        3: 'No charge',
        4: 'Dispute',
        5: 'Unknown',
        6: 'Voided trip'
    }

    # Обработка данных
    data_dict = {}
    for line in lines:
        month, payment_code, value = line.split()
        payment_code = int(payment_code)
        value = float(value)
        payment_type = mapping[payment_code]

        if month not in data_dict:
            data_dict[month] = {}

        if payment_type not in data_dict[month]:
            data_dict[month][payment_type] = []

        data_dict[month][payment_type].append(value)

    # Вычисление среднего значения
    result = []
    for month, payment_data in data_dict.items():
        for payment_type, values in payment_data.items():
            avg_value = sum(values) / len(values)
            result.append((month, payment_type, avg_value))

    return result





if __name__ == '__main__':
    input_file = "output.txt"
    result = reducer_map(input_file)
    df = pd.DataFrame(result, columns=["Month", "Payment type", "Tip average amount"])
    df.to_csv('result.csv', index=False)