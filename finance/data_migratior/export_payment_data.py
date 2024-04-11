import csv
import json
from datetime import datetime


def process_payment_data(file_name):
    with open(file_name) as file:
        reader = csv.DictReader(file)

        all_rows = []
        for row in reader:
            row = dict(row)

            withdraw_amt = row['Withdrawal Amt.']
            deposit_amt = row['Deposit Amt.']
            closing_amt = row['Closing Balance']
            ref_num = row['Chq./Ref.No.']

            row['withdraw_amt'] = float(withdraw_amt) if withdraw_amt else 0
            row['deposit_amt'] = float(deposit_amt) if deposit_amt else 0
            row['closing_balance'] = float(closing_amt) if closing_amt else 0
            row['ref_number'] = ref_num if ref_num else None

            row['datestamp'] = datetime.strptime(row['Date'], '%d/%m/%y').strftime('%Y-%m-%d')
            row['Value Dt'] = datetime.strptime(row['Value Dt'], '%d/%m/%y').strftime('%Y-%m-%d')

            del row['Withdrawal Amt.']
            del row['Deposit Amt.']
            del row['Closing Balance']
            del row['Date']
            del row['Chq./Ref.No.']

            row = {k.lower().replace(' ', '_'): v for k, v in row.items()}
            all_rows.append(row)
    return all_rows
