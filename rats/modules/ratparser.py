# RATS GDS.... determines the data format....
# if each output is 16 bit, each sample would have 2 bytes : 00 00
# if each output is 32 bit, each sample would have 4 bytes : 00 00 00 00.. must join data in lumps of

from rats.modules.RATS_CONFIG import packet_structure, rats_input
import pandas as pd
import numpy as np
filename = 'gds_000C00_30_14092021_1520.txt'
series_stream = []

def create_first_frame(filename):
    with open(filename, 'r') as f:
        line = f.readline()
        while line:
            series_stream.append(line.strip())
            line = f.readline()

    reject_characters = ['-', ']', ':']
    df = pd.DataFrame(series_stream,columns=['bytes'])
    df = df[~df['bytes'].str.contains('|'.join(reject_characters))]
    df['index_count'] = df.index.values
    deltas = df['index_count'].diff()[1:]
    gaps = deltas[deltas>1]
    packet_number_dictionary ={}
    for i in range(len(gaps.index.values)):
        packet_number_dictionary[gaps.index.values[i]] = i
    df['packet_number'] = df['index_count'].map(packet_number_dictionary)
    df.reset_index(inplace=True)
    df.fillna(method='ffill',inplace=True)
    df.drop('index_count',axis=1,inplace=True)
    return(df)

def partition_packet_data(dataframe):
    data = dataframe.bytes.tolist()
    stream = ' '.join(data).split()
    counter = 0
    packet_dictionary = {}
    for i in packet_structure:
        if i == 'data':
            bytes = stream[counter:]
        else:
            bytes = stream[counter:counter+packet_structure[i]]

        bytes = ' '.join(bytes)
        packet_dictionary[i] = bytes
        counter += packet_structure[i]

    dataframe = pd.DataFrame.from_records([packet_dictionary])

    return(dataframe)

def validate_initial_partition(dataframe):
    """
    Take output from previous wrapper and make sure packet_number and packet_count line up, then delete packet_number
    """
    comp1 = dataframe['packet_number'].apply(int)+1
    if comp1.equals(dataframe['packet_count'].str.replace(' ','').apply(int,base=16)):
        print('Valid')
    else:
        print('nope')

    return dataframe.drop('packet_number',axis=1)


def generate_final_frame(dataframe, rats_input={}, protocol_fudge = 0):
    # TODO: remove protocol fudge when the bug is fixed in the protocol number
    input_bytes = None
    gds_protocol_version = dataframe['rats_gds_protocol_version'].iloc[0].replace(' ','')
    if (int(gds_protocol_version,16) - protocol_fudge) > 0:
        input_bytes = 4
    else:
        input_bytes = 2
    # now need to pull number of active EDBs...
    active_edb_flags = f"{int(dataframe['rats_capture_enable'].iloc[0].replace(' ',''), 16):0<b}"
    flaglist = [i+1 for i, x in enumerate(active_edb_flags) if x == '1']
    bytes = dataframe['data'].iloc[0].split()
    bytes = [bytes[i:i + input_bytes] for i in range(0, len(bytes), input_bytes)]
    bytes = [''.join(x) for x in bytes]
    bytes = [bytes[i:i + len(flaglist)] for i in range(0, len(bytes), len(flaglist))]
    df_dict = dataframe.iloc[0].drop('data').to_dict()
    df_dict['data'] = bytes

    for i in df_dict:
        if i != 'data' and type(df_dict[i]) == str:
            # print(type(df_dict[i]))
            df_dict[i] = df_dict[i].replace(' ','')

    # propagate sample rate into time column
    packet_start_time = int(df_dict['time'].replace(' ',''),16)
    number_of_samples = len(bytes)
    sample_rate = int(df_dict['rats_sample_rate'].replace(' ',''),16)
    propagated_time_column = [packet_start_time + (i*sample_rate) for i in range(number_of_samples)]

    # generate bufiss and sip columns
    llc_edb_index = flaglist.index(rats_input['edb'])
    siplist = []
    bufisslist = []

    #TODO: tidy up this horrendous logical fudge
    for i in bytes:
        binary_llc_edb = f"{int(i[llc_edb_index],16):0>16b}"
        siplist.append(int(binary_llc_edb[15-rats_input['llc_bit']]))
        bufisslist.append(int(binary_llc_edb[15-rats_input['bufiss_bit']]))


    df_dict['time'] = propagated_time_column
    df_dict['rats_capture_enable'] = [flaglist]
    df_dict['bufiss'] = bufisslist
    df_dict['sip'] = siplist

    df = pd.DataFrame(dict([(k,pd.Series(v)) for k,v in df_dict.items()]))
    df.drop('level_1',axis=1,inplace=True)
    df.fillna(method='ffill',inplace=True)

    df = df.set_index(['rats_gds_protocol_version','payload_size','packet_count','time','rats_sample_rate',
                       'llc_trigger_count','function_number','sample_number','barcode_hash','retention_time',
                       'reserved','bufiss','sip']).apply(pd.Series.explode).reset_index()

    df = df[df['rats_capture_enable'] != rats_input['edb']]

    # TODO: convert to generic function and pass list of columns
    df['data'] = df['data'].apply(int, base=16)
    df['function_number'] = df['function_number'].apply(int, base=16)
    df['llc_trigger_count'] = df['llc_trigger_count'].apply(int,base=16)
    df['packet_count'] = df['packet_count'].apply(int,base=16)
    df['rats_capture_enable'] = df['rats_capture_enable'].astype(int)
    df['llc_trigger_count'] = df['llc_trigger_count'].astype(int)
    # df.to_feather(f'../feathereddataframes/{filename}.feather')
    return df

def generate_wrapper(dataframe):
    return generate_final_frame(dataframe,rats_input=rats_input,protocol_fudge=240)

def find_outliers(dataframe):
    dataframe = dataframe.drop(0,level='sip')
    pivot = pd.pivot_table(dataframe, values='data', index=['function_number', 'llc_trigger_count'])
    markers = []  # initialise markers variable
    for i in pivot.index.get_level_values(
            'function_number').unique().to_list():  # creates a list of all function numbers and loops over them
        mode = pivot.xs(i, level='function_number')['data'].mode().to_list()[
            0]  # gets the mode of the average data of the current function
        markers += pivot.xs(i, level='function_number').index[
            pivot.xs(i, level='function_number')['data'] != mode].to_list()
    dataframe['anomalous'] = dataframe['llc_trigger_count'].isin(markers).astype(int)  # simple flag for anomalous data
    return(dataframe)

## Series of operations designed to execute wrappers for final frame
df = create_first_frame(filename)
partitioned_dataframe = df.groupby('packet_number').apply(partition_packet_data)
partitioned_dataframe.reset_index(inplace=True)
partitioned_dataframe = validate_initial_partition(partitioned_dataframe)
final_frame = partitioned_dataframe.groupby('packet_count').apply(generate_wrapper)
final_frame = final_frame.droplevel(0).reset_index(drop=True)

meandf = final_frame[final_frame['sip'] == 0].groupby(['function_number','llc_trigger_count','rats_capture_enable']).mean()
meandf.reset_index(inplace=True)
# actually need to do a groupby, then get mode of each, then map that mode to the original df...

def generate_mode_column(dataframe,columns_to_group = ['function_number','llc_trigger_count','rats_capture_enable'],llc = 'sip'):
    meandf = dataframe[dataframe[llc]==0].groupby(columns_to_group).mean()
    meandf = meandf[['function_number', 'rats_capture_enable', 'data']]
    filterdf = meandf.groupby(['function_number', 'rats_capture_enable']).agg(pd.Series.mode)
    #


print(meandf.head())
meandf = meandf[['llc_trigger_count','function_number', 'rats_capture_enable', 'data']]

filterdf = meandf.groupby(['function_number', 'rats_capture_enable']).agg(pd.Series.mode)
filterdf.reset_index(inplace=True)
print(filterdf.head())
print(filterdf.loc[(filterdf['function_number'] == 1286) & (filterdf['rats_capture_enable'] == 2), 'data'].tolist()[0])
print(meandf.function_number.unique())
print(meandf.rats_capture_enable.unique())
functions = meandf.function_number.unique()
edbs = meandf.rats_capture_enable.unique()

final_frame['filter'] = 0
for i in functions:
    for j in edbs:
        final_frame['filter'] = np.where((final_frame['function_number'] == i) & (final_frame['rats_capture_enable'] == j),
                                    filterdf.loc[(filterdf['function_number'] == i) & (filterdf['rats_capture_enable'] == j), 'data'].tolist()[0],
                                    final_frame['filter'])

print(final_frame.head())

# TODO: find outliers in dataframe with respect to function number etc...
