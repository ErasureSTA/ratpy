import pandas as pd
import rats.modules.topoparser as topo
import platform
import pathlib
import linecache
import numpy as np
from datetime import datetime
from collections import Counter
import plotly_express as px

if __name__ == "__main__":
    from rats.modules.RATS_CONFIG import packet_structure as _packet_structure


if platform.system() == 'Windows':
    splitchar = '\\'
else:
    splitchar = '/'
packagepath = pathlib.Path(__file__).parent.parent.resolve()


class RatParse:

    def __init__(self, filename):
        self.filename = filename
        try:
            self.packet_markers = self.packet_markers() # calls function to assign self.packet_markers
            self.active_edbs = self.determine_active_edbs(self.packet_markers)
            self.sample_rate = self.determine_sample_rate()  # calls function to assign self.sample_rate
            self.scaling_data, self.board = self.parse_topo_data()  # boolean false if this fails need to set some defaults if it is false
            self.dataframe = self.dataframe_output()
            self.verified = True
        except Exception as e:
            self.verified = False
            print('The file that you tried to parse is not recognised as a RATS file')
            print(f'Parsing failed with exception: \n {e}')

    # =================================================================================================================
    # ------------------------- PACKETMARKERS FUNCTION ----------------------------------------------------------------
    # =================================================================================================================
    def packet_markers(self):
        """ Docstring.
        Determine which lines denote the start and end of each packet in the RATS files
        :return: list of integer pairs identifying on which line each packet in the file starts and ends
        """
        list_of_first_characters = []
        with open(self.filename, 'r') as f:
            line = f.readline()
            totallines = 0
            # if line contains item in rejectchars, then it should be ignored for parsing purposes...
            rejectchars = ['-', ']', ':']
            series_stream = []
            rats_stream = []

            while line:
                series_stream.append(line.strip())
                line = f.readline()
            f.seek(0)
            # list of acceptable characters for start of line
            # acceptchars = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F']






            count = 0
            packets = []

            # TODO: make this less complex by implementing storage as numpy array, then filter and send index to list


            # while count < totallines:
            #     packetbound = []
            #     line = f.readline()
            #     count += 1
            #     if line[0] in acceptchars:
            #         packetbound.append(count)
            #         while line[0] in acceptchars:
            #             line = f.readline()
            #             count += 1
            #             if count > totallines:
            #                 break
            #         packetbound.append(count - 1)
            #         if packetbound[1] - packetbound[0] < 2:  # assume we need at least 2 lines in packet for a full CoC
            #             pass
            #         else:
            #             packets.append(packetbound)
        f.close()

        return packets

    # ==================================================================================================================
    # ------------------------- SAMPLERATE DETERMINATION FUNCTION ------------------------------------------------------
    # ==================================================================================================================
    # # DEPRACATED.....
    # def determine_sample_rate(self):
    #     """
    #     determine the sample rate of the rats acquisition - assume that the mode of the
    #     :param bounds: output from packet_markers§
    #     :param bits: will be used in future release to affect the parsing functionality
    #     :return: float, determined sample rate for the RATS file
    #     TO DO: integrate ability to change the acceptable format for 32 bit input
    #     """
    #     samplerates = []
    #
    #     # Perform operation 10 times, then determine and return the 10 results as a list
    #     for i in range(10):
    #         pack = []
    #         for j in range((self.packet_markers[i][1] - self.packet_markers[i][0]) + 1):
    #             line = linecache.getline(self.filename, self.packet_markers[i][0] + j)  # fast way to read
    #             line = line.strip()  # strip preceeding and tailing characters
    #             byts = line.split()  # split the line into bytes
    #             for k in byts:  # for each byte
    #                 pack.append(k.strip())  # append to a list for downstream processing
    #
    #         lookuppack = []
    #
    #         for j in range((self.packet_markers[i + 1][1] - self.packet_markers[i + 1][0]) + 1):
    #             line = linecache.getline(self.filename, self.packet_markers[i + 1][0] + j)  # fast way to read
    #             line = line.strip()  # strip preceeding and tailing characters
    #             byts = line.split()  # split the line into bytes
    #             for k in byts:  # for each byte
    #                 lookuppack.append(
    #                     k.strip())  # append to a list for downstream processing
    #
    #         reftime = ''.join(lookuppack[:4])
    #         time = ''.join(pack[:4])
    #
    #         reftime = int(reftime, 16)
    #         time = int(time, 16)
    #
    #         duration = reftime - time
    #
    #         dat = ''.join(pack[24:])
    #         n = self.active_edbs.index(31) + 1
    #         # need a separate chunkdata function, it seems... this sample rate thing won't work out if the bit rates
    #         # aren't right...
    #         chunkdata = [dat[i:i + n * 4] for i in range(0, len(dat), n * 4)]
    #         samplerates.append(duration / len(chunkdata))
    #
    #     c = Counter(samplerates)  # produces object to count number of elements in the list
    #     samplerate = c.most_common(1)[0][0]  # the most common sample rate in the dataset is likely correct one
    #
    #     return samplerate

    # TODO: Sample rate is now provided directly in the packet at bytes 11 and 12 - implement parse here

    # ==================================================================================================================
    # ------------------------- ACTIVE EDB DETERMINATION FUNCTION-------------------------------------------------------
    # ==================================================================================================================

    def determine_active_edbs(self):
        pack = []
        for i in range((self.packet_markers[0][1] - self.packet_markers[0][0]) + 1):
            line = linecache.getline(self.filename, self.packet_markers[0][0] + i)  # fast way to read
            line = line.strip()  # strip preceeding and tailing characters (including /n)
            byts = line.split()  # split the line into bytes
            for j in byts:  # for each byte
                pack.append(j.strip())

        flags = ''.join(pack[20:24])
        flags = f'{int(flags, 16):0<32b}'  # convert flags to binary string... should find a way to pull the format from the ini file
        flaglist = [31 - i for i, x in enumerate(flags) if x == '1'] # here, 31 should also be set by the ini file...
        flaglist.reverse()
        return flaglist

    # =================================================================================================================
    # ------------------------- PACKET PARSER -------------------------------------------------------------------------
    # =================================================================================================================
    def read_packet(self, packnum):

        # TODO: Implementation of proper distinction of bits per EDB

        """
        Parse a single packet from a file and return its data in the form of a pandas dataframe
        :param packnum: number of the packet to parse
        :param samplerate: output of samplerate(self) function above
        :param bounds: output of packetnumbers(self) function above
        :param bits: will be used in future releases to facilitate proper parsing
        :return: dictionary containing the information within the packet


        logic.... determine number of packets? go to last packet and read packet number??
        """

        # seems to me like there's no need to separate this step from determine packet markers... might even
        # be able to do a one-shot to dataframe manipulation if I plan this carefully...
        pack = []
        for i in range((self.packet_markers[packnum][1] - self.packet_markers[packnum][0]) + 1):
            line = linecache.getline(self.filename, self.packet_markers[packnum][0] + i)  # fast way to read
            line = line.strip()  # strip preceeding and tailing characters (including /n)
            byts = line.split()  # split the line into bytes
            for j in byts:  # for each byte
                pack.append(j.strip())  # append to a list for downstream processing

        # so I'm making a big list of bytes, then latter joining them all together in a string... then splitting them
        # up again..... hmm

        # ==============================================================================================================
        #   Parse bytes as per format - may need to be updated depending on final RATS data file format
        # ==============================================================================================================

        # here's the code that will bring in the config data and help to break up the packet...

        packet_dictionary = {}
        byte_marker = 0
        for i in _packet_structure:
            if i == 'data_crc':
                break
            packet_dictionary[i] = ''.join(pack[byte_marker:byte_marker+_packet_structure[i]])
            byte_marker += _packet_structure[i]
        dat = ''.join(pack[24:-1])
        crc = ''.join(pack[-1:])


        # time = ''.join(pack[:4])
        # llctrigcount = ''.join(pack[4:8])
        # function = ''.join(pack[8:10])
        # samplenum = ''.join(pack[10:12])
        # bcodehsh = ''.join(pack[12:16])
        # tblnum = ''.join(pack[16:18])
        # tblid = ''.join(pack[18:20])
        # dat = ''.join(pack[24:])


        # let's store what's needed as... blah.. what do I currently do..

        # =============================================================================================================
        #   Format the outputs appropriately
        # =============================================================================================================
        # TODO: Overcome failure if RATS is valid bit there's some kind of topo file matching error

        if self.scaling_data:
            bytes_per_edb = self.scaling_data['bytes']
        else:
            bytes_per_edb = {i: 4 for i in self.active_edbs}

        bytes_per_cycle = 0
        bytes_per_cycle += bytes_per_edb[i]

        # if the topo files turned up something useable...
        count = 0
        data = []

        while count < len(dat):
            for i in self.active_edbs:
                data.append(dat[count:count+bytes_per_edb[i]])
                count += bytes_per_edb[i]

        # =============================================================================================================
        #   Construct dictionary for output
        # =============================================================================================================
        edblist = []
        datalist = []
        packetcycle = []
        timestamps = []
        scanflag = []

        number_of_samples = int(len(data)/len(self.active_edbs))  # determine how many edb samples there are in this packet

        for i in range(number_of_samples):  # for every cycle, make sure that we have data for each edb
            edblist += self.active_edbs  # every cycle will have a full complement of EDB outputs
            data = [int(x, 16) for x in data]  # convert data to human numbers
            datalist += data  # add the data to the list
            packetcycle += [i + 1]*len(self.active_edbs)
            timestamps += [int(time,16) + i * self.sample_rate]*len(self.active_edbs)
            flag = 1 if data[-1] == 1 else 0
            scanflag += [flag]*len(self.active_edbs)  # keep a record of whether this data is interscan data or scan data

        # extending these lists now makes the later concatenation of dictionaries possible in subsequent code;
        # the dictionaries all get much bigger but the time saving facilitated by this is about 40x
        packnum = [packnum]*len(datalist)
        llctrigcount = [llctrigcount]*len(datalist)
        function = [function]*len(datalist)
        samplenum = [samplenum]*len(datalist)
        tblnum = [tblnum]*len(datalist)
        tblid = [tblid]*len(datalist)
        bcodehsh = [bcodehsh]*len(datalist)

        packet_dictionary = dict(packet=packnum, llc=llctrigcount, function=function, sample=samplenum,
                          tablenumber=tblnum, tableid=tblid, barcodehash=bcodehsh, cycle=packetcycle,
                          scanflag=scanflag, edb=edblist, data=datalist, time=timestamps)

        return packet_dictionary

    # =================================================================================================================
    # ------------------------- TOPO/EDS PARSER -----------------------------------------------------------------------
    # =================================================================================================================
    def parse_topo_data(self):

        try:
            netid = self.filename.split(splitchar)[-1]
            netid = netid.split('.')[0]  # everything before the extension
            topodata = topo.extractscale(netid, self.active_edbs)

            return topodata

        except Exception as e:
            return False, False # topodata ouputs 2 values, which are unpacked in init. Return 2 values here to maintain function
            print(e)

    # =================================================================================================================
    # ------------------------- CONSTRUCT DATAFRAME FOR WHOLE FILE ----------------------------------------------------
    # =================================================================================================================
    def dataframe_output(self):
        """
        Formalises all relevant processes in the class to produce a final dataframe to save and operate on
        :return: Dataframe containing all parsed packet data

        Run time for an ~200 mb file is < 2min


        Serious questions need to be answered here about how the dataframe should be stored... efficiency of exploding
        dataframe rather than storing in long format.... logic, logic....

        n repeats of edb list can now be worked out from packet header.
        """

        # TODO: run through datasheets first to determine bit output of EDBs, then work out how to partition those
        #  properly in readpacket..

        print(f'generating dataframe for {self.filename}')

        starttime = datetime.now()

        print('ratparser is concatenating the dataframes')

        dictlist = [self.read_packet(i)
                    for i in range(len(self.packet_markers))]
        dfdict = {}

        # This code takes the list of dictionaries and stitches them into one big one, ready to transfer to a dataframe
        for listItem in dictlist:
            for key, value in listItem.items():  # Loop through all dictionary elements in the list
                if key in list(dfdict):  # if the key already exists, append to new
                    for entry in value:
                        dfdict[key].append(entry)
                else:  # if it's a new key, simply add to the new dictionary
                    dfdict[key] = value

        df = pd.DataFrame(dfdict)

        print('ratparser is done concatenating the dataframes')

        # do conversions to readable ints here
        # TODO: work out how to vectorise this
        df['llc'] = df['llc'].apply(int, base=16)
        df['function'] = df['function'].apply(int, base=16)
        df['tablenumber'] = df['tablenumber'].apply(int, base=16)
        df['tableid'] = df['tableid'].apply(int, base=16)

        # =============================================================================================================
        #   Find outliers
        # =============================================================================================================
        print('ratparser is finding outliers')
        df = df.set_index(['llc', 'packet', 'function', 'cycle', 'time', 'edb', 'scanflag']).sort_index()
        try:
            df = df.drop(1,
                         level='scanflag')  # here, we drop data for all cycles which are interscan packet cycles
        except Exception as e:  # this was probably MRM data, one sample per packet - no interscan data
            pass

        df.index.get_level_values('function').unique()  # grab all the function numbers
        df = df.reset_index()  # flatten the dataframe ready for pivot
        pivot = pd.pivot_table(df, values='data', index=['function', 'llc'])  # pivot table for relevant info
        markers = []  # initialise markers variable
        for i in pivot.index.get_level_values(
                'function').unique().to_list():  # creates a list of all function numbers and loops over them
            mode = pivot.xs(i, level='function')['data'].mode().to_list()[
                0]  # gets the mode of the average data of the current function
            markers += pivot.xs(i, level='function').index[
                pivot.xs(i, level='function')['data'] != mode].to_list()  # wherever the average data deviates

        df['anomalous'] = df['llc'].isin(markers).astype(int)  # simple flag for anomalous data
        df['anomalous'] = df['llc'].isin(markers).astype(int)  # simple flag for anomalous data

        print('ratparser is done looking for outliers')

        # convert columns to categories for big memory savings (storage and speed)
        cols = ['packet', 'llc', 'function', 'sample', 'tablenumber', 'tableid', 'scanflag', 'anomalous', 'barcodehash']

        def catcols(dframe, columns):
            for i in columns:
                dframe[i] = dframe[i].astype('category', copy=False)

            return df

        df = catcols(df, cols)

        # =============================================================================================================
        #   Scaling the data.. will import a topo parsing function, then run it on a unique list of EDBs...
        #   Want the code to rename the edbs to relevant data and scale the data values according to some factor
        # =============================================================================================================
        if self.scaling_data:

            df.loc[:, 'min'] = df['edb'].map(self.scaling_data['minimum'])
            df.loc[:, 'unit'] = df['edb'].map(self.scaling_data['units'])
            df.loc[:, 'scale'] = df['edb'].map(self.scaling_data['scalingfactor'])

            df.loc[:, 'edb'] = df['edb'].map(self.scaling_data['descriptions'])  # replace edb with description rather than vague
            df.loc[:, 'data'] = df['min'] + (df['data'] * df['scale'])  # replace data with appropriate value
            df.loc[:, 'board'] = self.board
            df['board'] = df['board'].astype('category')
        else:
            df['board'] = 'NO MATCH FOUND IN TOPO FILES'

        # ==============================================================================================================
        print(f'Dataframe construction completed in: {datetime.now() - starttime}')
        print(f'dataframe for {self.filename} uses {df.memory_usage().sum() / 10e6} Mb in memory')

        return df


# ======================================================================================================================
# ------------------------- TEST CASE ----------------------------------------------------------------------------------
# ======================================================================================================================
def test_case(absolutepath, file, scopestart=0, scopeend=100, show=False):
    """
    Tests all aspects of the ratparser class and proves functionality by plotting relevant data
    and saving the output dataframe
    :param absolutepath: Absolute path to the RATS file
    :param file: File name of the RATS file
    :param scopestart: packet number at the lower bound of the scope plot
    :param scopeend: packet number at the upped bound of the scope plot
    :param show: bool expression to determine whether to display plots (True) or not (False)
    :return: if show is True, then 3 plot types will be displayed in the browser
    """

    try:
        df = pd.read_feather(f'../feathereddataframes/{file}.feather')
    except Exception:
        testclass = RatParse(absolutepath)
        df = testclass.dataframe

    print(df.head())






# UNCOMMENT BELOW, MODIFY PATHS AS APPROPRIATE AND RUN THIS FILE TO TEST
# ================================================
start = 10
end = 100
file = '5.txt'
# test_case(f'/users/steve/documents/workwaters/{file}',file,start,end,show=True)
