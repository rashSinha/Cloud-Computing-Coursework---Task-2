import csv
import pandas as pd

# Define mapper and reducer functions 
def mapper(record):
    passenger_id = record[0]
    yield (passenger_id, 1)

def reducer(key, values):
    total_flights = sum(values)
    yield (key, total_flights)

def run_sequential():
    # Read the csv file and create a list of tuples
    with open('AComp_Passenger_data_no_error(1).csv', 'r') as f:
        reader = csv.reader(f)
        next(reader) # skip header row
        data = [(row[0],) for row in reader]

    # Run sequential implementation with shuffler here
    results_seq = {}
    for record in data:
        for key, value in mapper(record):
            if key not in results_seq:
                results_seq[key] = []
            results_seq[key].append(value)

    output_seq = [(key, sum(values)) for key, values in results_seq.items()]

    # Save sequential output to csv file
    df_seq = pd.DataFrame(output_seq, columns=['Passenger ID', 'Number of Flights'])
    df_seq = df_seq.sort_values(by='Number of Flights', ascending=False)
    df_seq.to_csv('output_seq.csv', index=False)
