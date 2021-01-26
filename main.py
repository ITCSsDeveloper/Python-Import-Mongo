import pymongo
import datetime
import model as la_model
import logging
import os


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
                            
class ImportToMongo :
    __pid = None
    __myclient = None
    __mydb = None
    __mycol = None
    
    def __init__(self):
        logging.debug(F'Init Import To Mongo.')
        conn_str = 'mongodb://root:example@localhost:27017'
        
        # Set PID
        self.__pid = os.getpid()
        logging.debug(F'PID = {self.__pid}' )
        
        # Setup Pymongo Connection String
        self.__myclient = pymongo.MongoClient(conn_str)
        self.__mydb = self.__myclient['gcc'] # Create Database
        self.__mycol = self.__mydb['gcc']    # Create Collection
        self.__mycol.delete_many({})         # Clear Collection
        
        logging.debug(F'DB Connected host={self.__myclient.HOST}:{self.__myclient.PORT}')
        pass
    
    def start(self, file_name): 
        row = 0             # Current Row Insert Per Cycle
        limit = 10          # Limit Row Insert Per Cycle (Recommend 10000 rows per cpu core)
        la_list_temp = []   # Temporary files
        row_inserted = 0    # Total Row Inserted 
        header = None       # var for keep header file
        footer = None       # var for keep footer file

        num_lines = sum(1 for line in open(file_name))  # Check Total Lines
        
        logging.debug(F'Total Lines {str(num_lines)}')
        logging.debug(F'Start Import')

        with open(file_name , mode="r") as f:
            for line in f:
                data = line.split('|')          # Split With
                la_model_temp = la_model.body() # New Object Model

                if data[0] == 'HT':             # Check Header and Keep
                    data[2] = data[2].replace('\n','')
                    header = data
                    continue
                elif data[0] == 'FT' :          # Check Footer and Keep
                    data[1] = data[1].replace('\n','')
                    footer = data
                    pass
                elif data[0] == 'DT': # Check Data and Keep
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
                
               
                # Insert To MongoDB
                if (row >= limit) or (data[0] == 'FT' and row != 0) :
                    self.__mycol.insert_many(la_list_temp)     
                    row_inserted += row
                    
                    logging.debug(F'Inserted = {str(row)} {str(row_inserted)}/{str(num_lines)}')
                    row = 0
                    la_list_temp = []
                row += 1
                    
                    
        
        logging.debug(F'-------------------------------------------')
        logging.debug(F'Complete')
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