import pymongo
import logging
import os
import time
import sys

# Step1. Config MongoDB
mo_host = 'localhost'      
mo_user = 'mongodb'        
mo_pass = 'Password12345'
mo_port = '27017'        

# Get Script path runing
script_path = os.path.dirname(os.path.abspath(__file__))

# Setup Logging
logFormatter = "%(asctime)s %(levelname)s: %(message)s"

class ImportToMongo :
    __pid = None
    __myclient = None
    __mydb = None
    __mycol = None

    __start_time = None
    __stop_time = None

    __limit = 0
    __header = "HT"
    __body = "DT"
    __footer = "FT"

    __file_name = ""
    __map_file_name = ""

    __mo_collection = ""
    __mo_database = ""
    __log_name = ""


    
    def __init__(self):
        # Get args
        self.get_args()

        if self.__log_name == '':
            logging.debug(F'-log_name is required')
            sys.exit()


        logging.basicConfig( filename=F'{script_path}/logs/{self.__log_name}',
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

        # Check args
        if self.__file_name == '':
            logging.debug(F'-file_name is required')
            sys.exit()
        if self.__map_file_name == '':
            logging.debug(F'-map_file_name is required')
            sys.exit()
        if self.__limit == 0:
            logging.debug(F'-limit is required')
            sys.exit()
        if self.__mo_database == '':
            logging.debug(F'-mo_database is required')
            sys.exit()
        if self.__mo_collection == '':
            logging.debug(F'-mo_collection is required')
            sys.exit()
       
        if self.__mo_database == '':
            logging.debug(F'-mo_database is required')
            sys.exit()
        if self.__mo_collection == '':
            logging.debug(F'-mo_collection is required')
            sys.exit()
       
        logging.debug(F'Init Import To Mongo.')
        self.__start_time = time.time()
        
        # Set PID
        self.__pid = os.getpid()
        logging.debug(F'PID = {self.__pid}' )


        logging.debug(F'Init Import To Mongo.')
        self.__start_time = time.time()
        
        # Set PID
        self.__pid = os.getpid()
        logging.debug(F'PID = {self.__pid}' )

        
        # Setup Pymongo Connection String
        conn_str = F'mongodb://{mo_user}:{mo_pass}@{mo_host}:{mo_port}'
        self.__myclient = pymongo.MongoClient(conn_str)
        self.__mydb = self.__myclient[self.__mo_database] # Create Database
        self.__mycol = self.__mydb[self.__mo_collection]    # Create Collection
        self.__mycol.delete_many({})         # Clear Collection
        logging.debug(F'DB Connected host={self.__myclient.HOST}:{self.__myclient.PORT}')
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
            if "-limit=" in a: 
                self.__limit = int(a.split('=')[1])
            if "-header=" in a: 
                self.__header = str(a.split('=')[1])
            if "-body=" in a: 
                self.__body = str(a.split('=')[1])
            if "-footer=" in a: 
                self.__footer = str(a.split('=')[1])
            if "-file_name=" in a: 
                self.__file_name = str(a.split('=')[1])
            if "-map_file_name=" in a: 
                self.__map_file_name = str(a.split('=')[1])

            if "-mo_database=" in a: 
                self.__mo_database = str(a.split('=')[1])
            if "-mo_collection=" in a: 
                self.__mo_collection = str(a.split('=')[1])
            if "-log_name=" in a: 
                self.__log_name = str(a.split('=')[1])

    def get_mapping_template(self) :
       isHeader= False,
       isBody = False,
       isFooter = False
       template = {
           "header" : [],
           "body" : [],
           "footer" : []
       }
       with open(self.__map_file_name, mode="r") as f:
           for line in f:
               line = line.strip()

               if(line == "#HEADER") : isHeader = True; continue
               elif(line=="#ENDHEADER"): isHeader = False; continue
               elif(line == "#BODY") : isBody = True; continue
               elif(line=="#ENDBODY"): isBody = False; continue
               elif(line == "#FOOTER") : isFooter = True; continue
               elif(line=="#ENDFOOTER"):isFooter = False; continue

               if isHeader == True :
                   template["header"].append(line)
                   continue
               elif isBody == True :
                   template["body"].append(line)
                   continue
               elif isFooter == True :
                   template["footer"].append(line)
                   continue
       return template

    def start(self): 
        file_name = self.__file_name
        map_file_name = self.__map_file_name

        row = 0                 # Current Row Insert Per Cycle
        limit = self.__limit    # Limit Row Insert Per Cycle (Recommend 10000 rows per cpu core)
        la_list_temp = []       # Temporary files
        row_inserted = 0        # Total Row Inserted 
        header = None           # var for keep header file
        footer = None           # var for keep footer file

        num_lines = sum(1 for line in open(file_name))  # Check Total Lines
        map_template = self.get_mapping_template()    

        logging.debug(F'-------------------------------------------')
        logging.debug(F'Total Lines {str(num_lines)}')
        logging.debug(F'File Name : {file_name}')
        logging.debug(F'Map File Name : {map_file_name}')
        logging.debug(F'Start Import')

        # Start Process
        with open(file_name , mode="r") as f:
            for line in f:
                data = line.split('|')          # Split With

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
                    try:
                        model_temp = {}
                        for i in range(len(data)):
                            model_temp[map_template['body'][i]] = data[i].strip().replace('\n','')
                        la_list_temp.append(model_temp)
                    except Exception as e:
                        logging.debug(F'Error : {e} >> {data}')
                        pass
                    

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
        logging.debug(F'File Name : {file_name}')
        logging.debug(F'Map File Name : {map_file_name}')

        logging.debug(F'Insert Limit : {limit}')
        logging.debug(F'Total Lines : {num_lines}')
        logging.debug(F'Total Inserted : {row_inserted}')
        logging.debug(F'Header : {header}')
        logging.debug(F'Footer : {footer}')
        logging.debug(F'-------------------------------------------')
        pass # End of Start

# Init App And Start Process
app = ImportToMongo()
app.start()
del(app)