import pandas as pd

class MiscUtils:
    def __init__(self):
        pass

    def spark_df_shape(self, df, name):
        print(f'Number of rows in {name} dataset: {df.count()}')
        print(f'Number of columns in {name} dataset: {len(df.columns)}')
    
    def clean_ports(self, infile):
        valid_port_dict = {}
        with open(infile, 'r') as fp:
            for line in fp:
                splits = line.split('=')
                code = splits[0].strip()
                code = code.replace(' ', ''); code = code.replace("'", "")
                port = splits[1].strip()
                port = port.replace(' ', '')
                port = port.replace(',', ', '); port = port.replace("'", "")
                valid_port_dict[code] = port
        return valid_port_dict
