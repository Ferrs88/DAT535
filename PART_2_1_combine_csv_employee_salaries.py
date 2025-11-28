import os
import pandas as pd

def combine_employee_salaries(input_directory, output_file):
    dataframes = []

    for filename in os.listdir(input_directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(input_directory, filename)

            df = pd.read_csv(file_path)

            # 2020 Overtime Pay; 2021 Overtime Pay; 2019 Overtime Pay; 2024 Overtime Pay; Overtime Pay; Overtime_Pay
            # 2024 Longevity Pay; 2019 Longevity Pay; 2021 Longevity Pay; 2020 Longevity Pay; Longevity_Pay

            for col in df.columns:
                column = col.lower()
                if column in ['2020 overtime pay', '2021 overtime pay', '2019 overtime pay', '2024 overtime pay', 'overtime pay', 'overtime_pay']:
                    df.rename(columns={col: 'OvertimePay'}, inplace=True)
                if column in ['2024 longevity pay', '2019 longevity pay', '2021 longevity pay', '2020 longevity pay', 'longevity pay', 'longevity_pay']:
                    df.rename(columns={col: 'LongevityPay'}, inplace=True)
                if column in ['department name', 'department_name']:
                    df.rename(columns={col: 'DepartmentName'}, inplace=True)
                if column in ['base salary', 'base_salary']:
                    df.rename(columns={col: 'BaseSalary'}, inplace=True)
                if 'year' not in df.columns:
                    if '2019' in filename:
                        df['Year'] = 2019
                    elif '2020' in filename:
                        df['Year'] = 2020
                    elif '2021' in filename:
                        df['Year'] = 2021
                    elif '2022' in filename:
                        df['Year'] = 2022
                    elif '2023' in filename:
                        df['Year'] = 2023
                    elif '2024' in filename:
                        df['Year'] = 2024
                    else:
                        df['Year'] = None
            dataframes.append(df)

    combined_df = pd.concat(dataframes, ignore_index=True)
    print ("Summary of combined data:")
    print (combined_df.info())
    combined_df.to_csv(output_file, index=False)

if __name__ == "__main__":
    input_directory = 'data/PART_2_1_CSV_EMPLOYEE_SALARIES'
    output_file = 'data/PART_3_csv/combined_employee_salaries.csv'
    combined_df = combine_employee_salaries(input_directory, output_file)