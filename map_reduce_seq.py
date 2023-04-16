import pandas as pd

# Define mapper and reducer functions 
def mapper(record):
    passenger_id = record[0]
    yield (passenger_id, 1)

def reducer(key, values):
    total_flights = sum(values)
    yield (key, total_flights)

def run_sequential():
    # Read the csv file and create a pandas dataframe
    df = pd.read_csv('AComp_Passenger_data_no_error(1).csv', header=None)

    # Run sequential implementation with shuffler here
    results_seq = {}
    for record in df.itertuples(index=False):
        for key, value in mapper(record):
            if key not in results_seq:
                results_seq[key] = []
            results_seq[key].append(value)

    output_seq = [(key, sum(values)) for key, values in results_seq.items()]

    # Save sequential output to csv file
    df_seq = pd.DataFrame(output_seq, columns=['Passenger ID', 'Number of Flights'])
    df_seq = df_seq.sort_values(by='Number of Flights', ascending=False)
    df_seq.to_csv('output_seq.csv', index=False)

    # Print passenger with the highest number of flights
    print(f"Passenger ID {df_seq.iloc[0]['Passenger ID']} has travelled the most with {df_seq.iloc[0]['Number of Flights']} flights.")

if __name__ == '__main__':
    run_sequential()
