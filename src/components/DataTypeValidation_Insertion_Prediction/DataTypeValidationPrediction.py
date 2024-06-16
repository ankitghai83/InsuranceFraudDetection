import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
import csv
from src.application_logging.logger import App_Logger


class dBOperation:
    # This class will be used to handle all sql operation
    def __init__(self):
        self.path="src/Prediction_Database/"
        self.badFilePath="src/Prediction_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath="src/Prediction_Raw_Files_Validated/Good_Raw"
        self.logger=App_Logger()

    def dataBaseConnection(self,DatabaseName):
        #This method creates the database with the given name and if Database already exists then opens the connection to the DB.


        try:
            conn=sqlite3.connect(self.path+DatabaseName+'.db')
            file=open("src/Prediction_Logs/DataBaseConnectionLog.txt","a+")
            self.logger.log(file,"Opened %s database successfully" % DatabaseName)
            file.close()
        except ConnectionError:
            file=open("src/Prediction_Logs/DataBaseConnectionLog.txt","a+")
            self.logger.log(file,"Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError

        return conn

    def createTableDb(self,DatabaseName,column_names):
        # This method will create the table in the given database to insert the data which are good data as per data validation step
        try:
            conn=self.dataBaseConnection(DatabaseName)
            cursor=conn.cursor()
            cursor.execute('DROP TABLE IF EXISTS Good_Raw_Data;')

            for key in column_names.keys():
                type=column_names[key]
                try:
                    cursor.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))

                except:
                    cursor.execute('CREATE TABLE Good_Raw_Data ({column_name} {dataType})'.format(column_name=key,dataType=type))
            conn.close()


            file=open("src/Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file,"Tables created successfully!!")
            file.close()

            file=open("src/Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file,"Closed %s database successfully" % DatabaseName)
            file.close()
            return conn

        except Exception as e:
            file=open("src/Prediction_Logs/DbTableCreateLog.txt",'a+')
            self.logger.log(file,"Error while creating table: %s " % e)
            file.close()
            conn.close()
            file=open("src/Prediction_Logs/DataBaseConnectionLog.txt",'a+')
            self.logger.log(file,"Closed %s database successfully" % DatabaseName)
            file.close()
            raise e


    def insertIntoTableGoodData(self,Database):
        # This method will insert data from Good_Raw folder to the table created above

        conn=self.dataBaseConnection(Database)
        cursor=conn.cursor()
        goodFilePath=self.goodFilePath
        badFilePath=self.badFilePath
        onlyfiles=[f for f in listdir(goodFilePath)]
        log_file=open("src/Prediction_Logs/DbInsertLog.txt", 'a+')

        for file in onlyfiles:
            try:
                with open(goodFilePath+'/'+file, 'r') as f:
                    next(f)
                    reader = csv.reader(f,delimiter='\n')
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                cursor.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=(list_)))
                                self.logger.log(log_file,"%s: File loaded successfully!!" % file)
                                conn.commit()

                            except Exception as e:
                                raise e


            except Exception as e:
                conn.rollback()
                self.logger.log(log_file,"Error while creating table: %s " % e)
                shutil.move(goodFilePath+'/'+file, badFilePath)
                self.logger.log(log_file,"File Moved Successfully %s" % file)
                log_file.close()
                conn.close()
                raise e

        log_file.close()
        conn.close()


    def selectingDatafromtableintocsv(self,Database):
        # This method will move the data from the database to .csv file at given location
        self.fileFromDB="src/Prediction_FileFromDB/"
        self.fileName='InputFile.csv'
        log_file=open("src/Prediction_Logs/ExportToCsv.txt","a+")
        try:
            conn=self.dataBaseConnection(Database)
            sqlSelect="SELECT * FROM Good_Raw_Data"
            cursor=conn.cursor()
            cursor.execute(sqlSelect)
            results = cursor.fetchall()

            #getting the header for csv file
            headers = [i[0] for i in cursor.description]

            # check if the fileFromDb folder exist
            if not os.path.isdir(self.fileFromDB):
                os.makedirs(self.fileFromDB)

            # Open CSV file for writing.
            csvFile = csv.writer(open(self.fileFromDB + self.fileName, 'w', newline=''), delimiter=',',lineterminator='\r\n', quoting=csv.QUOTE_ALL, escapechar='\\')
            # Adding data in the csv files
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file,"File exported successfully!!!")

        except Exception as e:
            self.logger.log(log_file,"File exporting failed. Error : %s" %e)
            raise e









