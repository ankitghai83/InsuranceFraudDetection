import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
import csv
from src.application_logging.logger import App_Logger


class dBOperation:
    # Description : This class is used to handle all the db/sql operations
    def __init__(self):
        self.path="src/Training_Database/"
        self.badFilePath="src/Training_Raw_files_validated/Bad_Raw"
        self.goodFilePath="src/Training_Raw_files_validated/Good_Raw"
        self.logger=App_Logger()

    def dataBaseConnection(self,DatabaseName):

        # Description : This method creates the database with the given name and if Database already exists then opens the connection to the DB.
        try:
            conn=sqlite3.connect(self.path+DatabaseName+'.db')

            file=open("src/Training_Logs/DataBaseConnectionLog.txt","a+")
            self.logger.log(file,"Opened %s database successfully" % DatabaseName)
            file.close()
        except ConnectionError:
            file=open("src/Training_Logs/DataBaseConnectionLog.txt","a+")
            self.logger.log(file,"Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return conn

    def createTableDb(self,DatabaseName,column_names):
        #Description : This method creates a table in the given database which will be used to insert the Good data after raw data validation.

        try:
            conn=self.dataBaseConnection(DatabaseName)
            c=conn.cursor()
            c.execute("select count(name) from sqlite_master where type='table' and name='Good_Raw_Data'")
            if c.fetchone()[0]==1:
                conn.close()
                file=open("src/Training_Logs/DbTableCreateLog.txt","a+")
                self.logger.log(file,"Tables created successfully!!")
                file.close()

                file=open("src/Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file,"Closed %s database successfully" % DatabaseName)
                file.close()

            else:
                for key in column_names.keys():
                    type=column_names[key]
                    # As the list of columns that we have been given in the training Batch file will be recorded as a part of initial discussion
                    # with client and stored in schema_training.json and schema_prediction.json file and with the help of rawValidation.py file
                    #we will get that column list. So, if table is there but conn.fetchone()[0] will returned None meaning no data in table above
                    # then we will alter table and add columns in that using TRY blockor try to create the table from scractch in except block
                    try:
                        conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                    except:
                        conn.execute('CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))

                conn.close()
                file=open("src/Training_Logs/DbTableCreateLog.txt","a+")
                self.logger.log(file,"Tables created successfully!!")
                file.close()

                file=open("src/Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file,"Closed %s database successfully!" % DatabaseName)
                file.close()

        except Exception as e:


            file=open("src/Training_Logs/DbTableCreateLog.txt","a+")
            self.logger.log(file,"Error while creating the table %s" % e)
            file.close()
            conn.close()
            file=open("src/Training_Logs/DatabaseConnectionLog.txt", 'a+')
            self.logger.log(file,"closed %s database successfully!" % DatabaseName)
            file.close()
            raise e


    def insertIntoTableGoodData(self,Database):

        # This method will insert Good data files located in Good_raw folder into the above created table.
        conn=self.dataBaseConnection(Database)
        goodFilePath=self.goodFilePath
        badFilePath=self.badFilePath
        onlyfiles=[f for f in listdir(goodFilePath)]
        log_file=open("src/Training_Logs/DbInsertLog.txt","a+")

        for file in onlyfiles:
            try:
                with open(goodFilePath+'/'+file,"r") as f:
                    next(f)
                    reader=csv.reader(f,delimiter="\n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=list_))
                                self.logger.log(log_file," %s: File loaded successfully!!" % file)
                                conn.commit()

                            except Exception as e:
                                raise e


            except Exception as e:
                conn.rollback()
                self.logger.log(log_file,"Error while creating table: %s " % e)
                shutil.move(goodFilePath+"/"+file,badFilePath)
                self.logger.log(log_file,"File Moved Successfully %s" % file)
                log_file.close()
                conn.close()

        conn.close()
        log_file.close()

    def selectingDatafromtableintocsv(self,Database):

        # This method will export the data from the database -> Table created above and push into a CSV file for next step in data modeling
        self.fileFromDb="src/Training_FileFromDB/"
        self.fileName="InputFile.csv"
        log_file=open("src/Training_Logs/ExportToCsv.txt","a+")
        try:
            conn=self.dataBaseConnection(Database)
            sqlSelect="SELECT * FROM Good_Raw_Data"
            cursor=conn.cursor()
            cursor.execute(sqlSelect)

            results=cursor.fetchall()
            # Get the header of the CSV file
            header=[i[0] for i in cursor.description]
            # Make directory input file used in model

            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open the CSV file for writing

            csvFile=csv.writer(open(self.fileFromDb+self.fileName,'w',newline=''),delimiter=',',lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')
            # Adding data in the excel csv file
            csvFile.writerow(header)
            csvFile.writerows(results)

            self.logger.log(log_file,"File exported successfully!!!")



        except Exception as e:
            self.logger.log(log_file,"File exporting failed. Error : %s")
            log_file.close()
            raise e










