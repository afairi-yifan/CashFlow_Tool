from Config.Location import location
import os

if location == 'YOURNAME':
    path_data = 'Data'
else:
    path_data = None


path_input = os.path.join(path_data, 'Input')
path_throughput = os.path.join(path_data, 'Throughput')
path_output = os.path.join(path_data, 'Output')