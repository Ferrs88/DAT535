import os
import pandas as pd

def combine_employee_salaries(input_directory, output_file):
    dataframes = []

    for filename in os.listdir(input_directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(input_directory, filename)

            df = pd.read_csv(file_path)

            dataframes.append(df)

    combined_df = pd.concat(dataframes, ignore_index=True)
    print ("Summary of combined data:")
    print (combined_df.info())
    combined_df.to_csv(output_file, index=False)

if __name__ == "__main__":
    input_directory = 'data/PART_2_2_SALARIES'
    output_file = 'data/PART_3_csv/combined_salaries.csv'
    combined_df = combine_employee_salaries(input_directory, output_file)