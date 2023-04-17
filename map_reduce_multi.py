import pandas as pd
import multiprocessing as mp
from functools import reduce
import graphviz

'''Define separate templates for mapper, shuffler and reducer 
to be called in a separate class PassengerDataAnalyzer'''

class Mapper:
    def __init__(self, data):
        self.data = data
        
    def __call__(self, x):
        return (x, 1)
    

class Shuffler:
    def __call__(self, mapper_out):
        data = {}
        for k, v in mapper_out:
            if k not in data:
                data[k] = [v]
            else:
                data[k].append(v)
        return data
    

class Reducer:
    def __call__(self, x, y):
        return x + y
    
'''Define a class that can later be used to create different 
instances of the passenger data for multiple files'''

class PassengerDataAnalyzer:
    def __init__(self, input_file_path):
        self.input_file_path = input_file_path
        self.mapper = Mapper(self._read_data())
        self.shuffler = Shuffler()
        self.reducer = Reducer()
        
    def _read_data(self):
        return pd.read_csv(self.input_file_path, header=None).iloc[:, 0].tolist()
        
    def analyze(self):
        dot = graphviz.Digraph(comment='MapReduce for Passenger Data Analysis')
        dot.node('input', 'Input')
        dot.node('map', 'Mapper')
        dot.node('shuffle', 'Shuffler')
        dot.node('reduce', 'Reducer')
        dot.node('output', 'Output')
        
        dot.edge('input', 'map')
        dot.edge('map', 'shuffle')
        dot.edge('shuffle', 'reduce')
        dot.edge('reduce', 'output')

        with mp.Pool(processes=mp.cpu_count()) as pool:
            # Map phase
            map_out = pool.map(self.mapper, self.mapper.data, chunksize=int(len(self.mapper.data)/mp.cpu_count()))
            
            # Shuffle phase
            reduce_in = self.shuffler(map_out)
            
            # Reduce phase
            reduce_out = {}
            for key, values in reduce_in.items():
                reduce_out[key] = reduce(self.reducer, values)
            
            # Convert output to pandas dataframe and sort in descending order
            df = pd.DataFrame(list(reduce_out.items()), columns=['Passenger ID', 'Number of Flights'])
            df = df.sort_values('Number of Flights', ascending=False)
            
            # Print passenger with the highest number of flights
            print(f"Passenger ID {df.iloc[0]['Passenger ID']} has travelled the most with {df.iloc[0]['Number of Flights']} flights.")
            
            # Save output to CSV file
            df.to_csv('output_mp.csv', index=False)

        dot.render('map_reduce_flowchart.gv', view=True)

if __name__ == '__main__':
    analyzer = PassengerDataAnalyzer('AComp_Passenger_data_no_error(1).csv')
    analyzer.analyze()
