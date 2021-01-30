import pymongo
import model as la_model
import logging
import os
import time
import sys


# Setup Logging
logFormatter = "%(asctime)s %(levelname)s: %(message)s"
logging.basicConfig( filename='logs/logs.log',
                    encoding='utf-8',
                    level=logging.DEBUG,
                    format=logFormatter,
                    filemode='a'
                    )
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter(logFormatter)
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# Connection
conn_str = 'mongodb://mongodb:Password12345@localhost:27017'

class ImportToMongo :
    __pid = None
    __myclient = None
    __mydb = None
    __mycol = None

    __start_time = None
    __stop_time = None

    __limit = 1
    __header = "HT"
    __body = "DT"
    __footer = "FT"

    def __init__(self):
        logging.debug(F'Init Import To Mongo.')
        self.__start_time = time.time()
        
        # Set PID
        self.__pid = os.getpid()
        logging.debug(F'PID = {self.__pid}' )
        
        # Setup Pymongo Connection String
        self.__myclient = pymongo.MongoClient(conn_str)
        self.__mydb = self.__myclient['gcc'] # Create Database
        self.__mycol = self.__mydb['gcc']    # Create Collection
        self.__mycol.delete_many({})         # Clear Collection
        logging.debug(F'DB Connected host={self.__myclient.HOST}:{self.__myclient.PORT}')

        # Get args
        self.get_args()
        logging.debug(F'args limit={self.__limit}')
        pass
    
    def time_convert(self, sec):
        mins = sec // 60
        sec = sec % 60
        hours = mins // 60
        mins = mins % 60
        return ("Time Lapsed = {0}:{1}:{2}".format(int(hours),int(mins),sec))

    def get_args(self):
        for a in sys.argv:
            print(a)
            if "-limit=" in a: 
                self.__limit = int(a.split('=')[1])
            if "-header=" in a: 
                self.__header = int(a.split('=')[1])
            if "-body=" in a: 
                self.__body = int(a.split('=')[1])
            if "-footer=" in a: 
                self.__footer = int(a.split('=')[1])

    def start(self, file_name): 
        row = 0                 # Current Row Insert Per Cycle
        limit = self.__limit    # Limit Row Insert Per Cycle (Recommend 10000 rows per cpu core)
        la_list_temp = []       # Temporary files
        row_inserted = 0        # Total Row Inserted 
        header = None           # var for keep header file
        footer = None           # var for keep footer file

        num_lines = sum(1 for line in open(file_name))  # Check Total Lines
        
        logging.debug(F'Total Lines {str(num_lines)}')
        logging.debug(F'Start Import')

        with open(file_name , mode="r") as f:
            for line in f:
                data = line.split('|')          # Split With
                la_model_temp = la_model.body() # New Object Model

                # Check Header and Keep
                if data[0] == self.__header:             
                    data[2] = data[2].replace('\n','')
                    header = data
                    continue
                # Check Footer and Keep
                elif data[0] == self.__footer:          
                    data[1] = data[1].replace('\n','')
                    footer = data
                    pass
                # Check Data and Keep
                elif data[0] == self.__body: 
                    la_model_temp.XXCORDTXXX = data[0]
                    la_model_temp.XXC = data[1]
                    la_model_temp.XXENTIFICATION_TXXX = data[2]
                    la_model_temp.XXD = data[3]
                    la_model_temp.XXRTHOFDXXX = data[4]
                    la_model_temp.XXTYPECXXX = data[5]
                    la_model_temp.XXSUBTYPECXXX = data[6]
                    la_model_temp.XXNXXX = data[7]
                    la_model_temp.XXANTEDXXX = data[8]
                    la_model_temp.XXINCIXXX = data[9]
                    la_model_temp.XXTSTBALAXXX = data[10]
                    la_model_temp.XXTTXXX = data[11]
                    la_model_temp.XXXINSTXXX = data[12]
                    la_model_temp.XXNINSTXXX = data[13]
                    la_model_temp.XXEQINSTXXX = data[14]
                    la_model_temp.XXSXXX = data[15]
                    la_model_temp.XXUEIXXX = data[16]
                    la_model_temp.XXEDXXX = data[17]
                    la_model_temp.XXXTDUEDXXX = data[18]
                    la_model_temp.XXGNDXXX = data[19]
                    la_model_temp.XXENDXXX = data[20]
                    la_model_temp.XXARTDXXX = data[21]
                    la_model_temp.XXTUREDXXX = data[22]
                    la_model_temp.XXAWDOWXXX = data[23]
                    la_model_temp.XXAWDOWNDXXX = data[24]
                    la_model_temp.XXSBDXXX = data[25]
                    la_model_temp.XXSBXXX = data[26]
                    la_model_temp.XXIDXXX = data[27]
                    la_model_temp.XXTEFFDXXX = data[28]
                    la_model_temp.XXNTEFFDXXX = data[29]
                    la_model_temp.XXTRATECXXX = data[30]
                    la_model_temp.XXTMXXX = data[31]
                    la_model_temp.XXRMALRXXX = data[32]
                    la_model_temp.XXNALTYRXXX = data[33]
                    la_model_temp.XXRXXX = data[34]
                    la_model_temp.XXACEPERXXX = data[35]
                    la_model_temp.XXACEPERIODXXX = data[36]
                    la_model_temp.XXRXXX = data[37]
                    la_model_temp.XXRXXX = data[38]
                    la_model_temp.XXRINTHXXX = data[39]
                    la_model_temp.XXVANCEINTERXXX = data[40]
                    la_model_temp.XXINFXXX = data[41]
                    la_model_temp.XXINTXXX = data[42]
                    la_model_temp.XXFXXX = data[43]
                    la_model_temp.XXLARYFXXX = data[44]
                    la_model_temp.XXPAYMETHODCODXXX = data[45]
                    la_model_temp.XXLLADDRCXXX = data[46]
                    la_model_temp.XXCXXX = data[47]
                    la_model_temp.XXSTTRNDXXX = data[48]
                    la_model_temp.XXDXXX = data[49]
                    la_model_temp.XXMINXXX = data[50]
                    la_model_temp.XXDXXX = data[51]
                    la_model_temp.XXMPRXXX = data[52]
                    la_model_temp.XXJECTCODXXX = data[53]
                    la_model_temp.XXTOBJCXXX = data[54]
                    la_model_temp.XXNSCXXX = data[55]
                    la_model_temp.XXDIXXX = data[56]
                    la_model_temp.XXDDUEDXXX = data[57]
                    la_model_temp.XXSTDEBTRESXXX = data[58]
                    la_model_temp.XXMRESTRUCTXXX = data[59]
                    la_model_temp.XXLLOVERDXXX = data[60]
                    la_model_temp.XXLLOVERINTRXXX = data[61]
                    la_model_temp.XXLLOVERPENRXXX = data[62]
                    la_model_temp.XXLLOVERSPREADRXXX = data[63]
                    la_model_temp.XXURCEXXX = data[64]
                    la_model_temp.XXAXXX = data[65]
                    la_model_temp.XXackStaXXX = data[66].replace('\n','')
                    la_list_temp.append(la_model_temp.__dict__)
                
                row += 1

                # Insert To MongoDB
                if (row >= limit) or (data[0] == self.__footer and row != 0) :
                    if  len(la_list_temp) > 0:
                        self.__mycol.insert_many(la_list_temp)     
                        row_inserted += len(la_list_temp)
                        
                        logging.debug(F'Inserted = {str(row)} {str(row_inserted)}/{str(num_lines)}')
                        la_list_temp = []
                        row = 0

                

        self.__stop_time = time.time()
        time_lapsed = self.time_convert((self.__stop_time - self.__start_time) )

        logging.debug(F'-------------------------------------------')
        logging.debug(F'Complete')
        logging.debug(F'Time lapsed : {time_lapsed}')

        logging.debug(F'Insert Limit : {limit}')
        logging.debug(F'Total Lines : {num_lines}')
        logging.debug(F'Total Inserted : {row_inserted}')
        logging.debug(F'Header : {header}')
        logging.debug(F'Footer : {footer}')
        logging.debug(F'-------------------------------------------')
        pass # End of Start

# Init App And Start Process
app = ImportToMongo()
app.start("./data/LA00000.GCC")
del(app)