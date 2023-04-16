import pandas as pd
import multiprocessing as mp
from functools import reduce

# Map function
def map_func(x):
    return (x, 1)

# Shuffle function
def shuffle(mapper_out):
    data = {}
    for k, v in mapper_out:
        if k not in data:
            data[k] = [v]
        else:
            data[k].append(v)
    return data

# Reduce function
def reduce_func(x, y):
    return x + y

if __name__ == '__main__':
    # Read input CSV file
    data = pd.read_csv('/Users/rashmilsinha/Downloads/Task 2/AComp_Passenger_data_no_error(1).csv', header=None)
    map_in = data.iloc[:, 0].tolist()
    
    with mp.Pool(processes=mp.cpu_count()) as pool:
        # Map phase
        map_out = pool.map(map_func, map_in, chunksize=int(len(map_in)/mp.cpu_count()))
        
        # Shuffle phase
        reduce_in = shuffle(map_out)
        
        # Reduce phase
        reduce_out = {}
        for key, values in reduce_in.items():
            reduce_out[key] = reduce(reduce_func, values)
        
        # Convert output to pandas dataframe and sort in descending order
        df = pd.DataFrame(list(reduce_out.items()), columns=['Passenger ID', 'Number of Flights'])
        df = df.sort_values('Number of Flights', ascending=False)
        
        # Print passenger with the highest number of flights
        print(f"Passenger ID {df.iloc[0]['Passenger ID']} has travelled the most with {df.iloc[0]['Number of Flights']} flights.")
        
        # Save output to CSV file
        df.to_csv('output_mp.csv', index=False)
