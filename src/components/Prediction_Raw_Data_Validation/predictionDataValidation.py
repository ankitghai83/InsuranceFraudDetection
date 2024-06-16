import json
import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import jason
import shutil
import pandas as pd
from sqlalchemy.sql.functions import now

from src.application_logging.logger import App_Logger

class Prediction_Data_validation:
    # This class shall be used for handling all the validation done on the Raw Prediction Data!!.
    def __init__(self,path):
        self.Batch_Directory =path
        self.schema_path='src/schema_prediction.json'
        self.logger=App_Logger()

    def valuesFromSchema(self):
        # This method extracts all the relevant information from the pre-defined "Schema" file like LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
        try:
            with open(self.schema_path,'r') as f:
                dic=json.load(f)
                f.close()
            pattern=dic['SampleFileName']
            LengthOfDateStampInFile=dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile=dic['LengthOfTimeStampInFile']
            column_names=dic['ColName']
            NumberofColumns=dic['NumberofColumns']

            file=open("src/Prediction_Logs/valuesfromSchemaValidationLog.txt",'a+')
            message="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(file,message)

            file.close()

        except ValueError:
            file=open("src/Prediction_Logs/valuesfromSchemaValidationLog.txt",'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file=open("src/Prediction_Logs/valuesfromSchemaValidationLog.txt",'a+')
            self.logger.log(file,"KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file=open("src/Prediction_Logs/valuesfromSchemaValidationLog.txt",'a+')
            self.logger.log(file,str(e))
            file.close()
            return e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):
        #This method contains a manually defined regex based on the "FileName" given in "Schema" file.. This is used to validate the filename of the prediction data sent by the client

        regex="['fraudDetection']+['\_'']+[\d_]+[\d]+\.csv"

        return regex

    def createDirectoryForGoodBadRawData(self):
        # This method help in creating the bad and the good directories that can store respective data after data validation
        try:
            path=os.path.join("src/Prediction_Raw_Files_Validated/"+"Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path=os.path.join("src/Prediction_Raw_Files_Validated/"+"Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
        except OSError as ex:
            file=open("src/Prediction_logs/GeneralLog.txt","a+")
            self.logger.log(file,"Error while creating Directory %s: " % ex)
            file.close()
            raise OSError

    def deleteExistingGoodDataPredictionFolder(self):
        # This method will delete the existing good data directory after data has been loaded into the Db as doing that is actually space optimozation
        try:
            path="src/Prediction_Raw_Files_Validated"
            if os.path.isdir(path+"/"+"Good_Raw/"):
                shutil.rmtree(path+"/"+"Good_Raw/")
                file=open("src/Prediction_logs/GeneralLog.txt","a+")
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as s:
            file=open("src/Prediction_logs/GeneralLog.txt","a+")
            self.logger.log(file,"Error while Deleting Directory %s: " % s)
            file.close()
            raise OSError

    def deleteExistingBadDataPredictionFolder(self):
        #This method will delete the existing bad data directory.
        try:
            path="src/Prediction_Raw_Files_Validated"
            if os.path.isdir(path+"/"+"Bad_Raw/"):
                shutil.rmtree(path+"/"+"Bad_Raw/")
                file=open("src/Prediction_logs/GeneralLog.txt","a+")
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()

        except OSError as s:
            file=open("src/Prediction_logs/GeneralLog.txt","a+")
            self.logger.log(file,"Error while Deleting Directory %s:" % s)
            file.close()
            raise OSError

    def moveBadFilesToArchiveBad(self):
        #This method deletes the directory made  to store the Bad Data after moving the data in an archive folder. We archive the bad files to send them back to the client for invalid data issue.
        now=datetime.now()
        date=now.date()
        time=now.strftime("%H%M%S")
        try:
            path="PredictionArchivedBadData"
            if not os.path.isdir(path):
                os.makedirs(path)
            source="src/Prediction_Raw_Files_Validated/Bad_Raw/"
            dest="src/PredictionArchivedBadData/BadData_"+str(date)+"_"+str(time)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            files=os.listdir(source)
            for f in files:
                if f not in os.listdir(dest):
                    shutil.move(source+f,dest)
            file=open("src/Prediction_logs/GeneralLog.txt","a+")
            self.logger.log(file,"Bad files moved to archive")
            path="src/Prediction_raw_Files_Validated/Bad_Raw/"
            if os.path.isdir(path):
                shutil.rmtree(path)
            self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
            file.close()


        except OSError as e:
            file=open("src/Prediction_logs/GeneralLog.txt","a+")
            self.logger.log(file,"Error while moving bad files to archive:: %s" % e)
            file.close()
            raise OSError


    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        # This function will validate the prediction file name after comparing schema regex pattren is used to do the validation.If name format do not match the file is moved to Bad Raw Data folder else in Good raw data.
        # Delete bad & Good Directories in case last run was un-successfull
        self.deleteExistingGoodDataPredictionFolder()
        self.deleteExistingBadDataPredictionFolder()
        self.createDirectoryForGoodBadRawData()
        onlyfiles=[f for f in os.listdir(self.Batch_Directory)]
        try:
            f=open("src/Prediction_logs/nameValidationLog.txt","a+")
            for filename in onlyfiles:
                if (re.match(regex,filename)):
                    splitAtDot=re.split('.csv',filename)
                    splitAtDot=re.split('_',splitAtDot[0])
                    if len(splitAtDot[1])==LengthOfDateStampInFile:
                        if len(splitAtDot[2])==LengthOfTimeStampInFile:
                            shutil.copy("src/Prediction_Batch_Files/"+filename,"src/Prediction_Raw_Files_Validated/Good_Raw")
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("src/Prediction_Batch_Files/"+filename,"src/Prediction_Raw_Files_Validated/Bad_Raw")
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

                    else:
                        shutil.copy("src/Prediction_Batch_Files/"+filename,"src/Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

                else:
                    shutil.copy("src/Prediction_Batch_Files/"+filename,"src/Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()


        except Exception as e:
            f=open("src/Prediction_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f,"Error occured while validating file name!! %s" % e)
            f.close()
            raise e


    def validateColumnLength(self,NumberofColumns):
        #This function validates the number of columns in the csv files.
        #If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
        #If the column number matches, file is kept in Good Raw Data for processing.
        #The csv file is missing the first column name, this function changes the missing name to "Wafer".


        try:
            f=open("src/Prediction_Logs/columnValidationLog.txt","a+")
            self.logger.log(f,"Column Length Validation Started!!")
            for file in listdir("src/Prediction_Raw_Files_Validated/Good_Raw/"):
                csv=pd.read_csv("src/Prediction_Raw_Files_Validated/Good_Raw/"+file)
                if csv.shape[1]==NumberofColumns:
                    csv.to_csv("src/Prediction_Raw_Files_Validated/Good_Raw/" + file, index=None, header=True)
                else:
                    shutil.move("src/Prediction_Raw_Files_Validated/Good_Raw/"+file,"src/Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log("f","Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)


            self.logger.log(f,"Column Length Validation Completed!!")

        except OSError:
            f=open("src/Prediction_Logs/columnValidationLog.txt","a+")
            self.logger.log(f,"Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f=open("src/Prediction_Logs/columnValidationLog.txt","a+")
            self.logger.log(f,"Error Occured:: %s" % e)
            f.close()
            raise e

        f.close()


    def deletePredictionFile(self):
        if os.path.exists("src/Prediction_Output_File/Predictions.csv"):
            os.remove("src/Prediction_Output_File/Predictions.csv")



    def validateMissingValuesInWholeColumn(self):
        #This method will test if all the column in the csv file are missing then that file is not suitable for processing and should be transfer to bad data file folder
        try:
            f=open("src/Prediction_Logs/missingValuesInColumn.txt","a+")
            self.logger.log(f,"Missing Values Validation Started!!")
            for file in listdir("src/Prediction_Raw_Files_Validated/Good_Raw/"):
                csv=pd.read_csv("src/Prediction_Raw_Files_Validated/Good_Raw/"+file)
                count=0
                for columns in csv:
                    if (len(csv[columns])-csv[columns].count())==len(csv[columns]):
                        count+=1
                        shutil.move("src/Prediction_Raw_Files_Validated/Good_Raw/"+file,"src/Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    csv.to_csv("src/Prediction_Raw_Files_Validated/Good_Raw"+file,index=None, header=True)

        except OSError:
            f=open("src/Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f,"Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError

        except Exception as e:
            f=open("src/Prediction_Logs/missingValuesInColumn.txt")
            self.logger.log(f,"Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f=open("src/Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f,"Error Occured:: %s" % e)
            f.close()
            raise Exception()
        f.close()



