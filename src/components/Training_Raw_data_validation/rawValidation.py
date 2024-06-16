import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
import sys
from src.application_logging.logger import App_Logger
from src.exception import CustomException


class Raw_Data_validation:
    # path = "src\Training_Batch_Files\fraudDetection_021119920_010222.csv"

    ''' This class is used to handle all the vaidation done on the Raw Training Data
          Written By Ankit Ghai
          Version : 1.0
          Revision : None
    '''

    def __init__(self, path):
        self.Batch_Directory = path
        self.schema_path = 'src/schema_training.json'
        self.logger = App_Logger()

    def valueFromSchema(self):

        """
                        Method Name: valuesFromSchema
                        Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                        Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                        On Failure: Raise ValueError,KeyError,Exception

                         Written By: Ankit Ghai
                        Version: 1.0
                        Revisions: None

                                """

        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            file = open("src/Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message = "LengthOfDateStampInFile:: %s" % LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile + "\t" + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(file, message)
            file.close()

        except Exception as e:
            file = open("src/Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise CustomException(e, sys)

        except ValueError:
            file = open("src/Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("src/Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns

    def manualRegexCreation(self):
        """
                                Method Name: manualRegexCreation
                                Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                            This Regex is used to validate the filename of the training data.
                                Output: Regex pattern
                                On Failure: None

                                Written By: Ankit Ghai
                                Version: 1.0
                                Revisions: None

                                        """

        regex = "['fraudDetection']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        '''
        This method is used to create the directory for bad raw and good raw data after validating the training data file send by the client
        '''

        try:
            path = os.path.join("src/Training_Raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

            path = os.path.join("src/Training_Raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("src/Training_Logs/GeneralLog.txt", "+a")
            self.logger.log(file, "Error while creating Directory %s:" % ex)
            file.close()
            raise OSError



    def deleteExistingGoodDataTrainingFolder(self):

        '''
        Description: This method deletes the directory made  to store the Good Data
        after loading the data in the table. Once the good files are
        loaded in the DB,deleting the directory ensures space optimization.
        '''

        try:
            path = "src/Training_Raw_files_validated/"
            if os.path.isdir(path + "Good_Raw/"):
                shutil.rmtree(path + "Good_Raw/")
                file = open("src/Training_Logs/GeneralLog.txt", "a+")
                self.logger.log(file, "GoodRaw directory deleted successfully!!!")
                file.close()




        except OSError as s:
            file = open("src/Training_Logs/GeneralLog.txt", "a+")
            self.logger.log(file, "Error while Deleting Directory : %s" % s)
            file.close()
            raise OSError

    def deleteExistingBadDataTrainingFolder(self):
        '''
        Description: This method deletes the directory made to store the bad Data.
        '''

        try:
            path = "src/Training_Raw_files_validated/"
            if os.path.isdir(path + "Bad_Raw/"):
                shutil.rmtree(path + "Bad_Raw/")
                file = open("src/Training_Logs/GeneralLog.txt", "a+")
                self.logger.log(file, "BadRaw directory deleted before starting validation!!!")
                file.close()

        except OSError as s:
            file = open("src/Training_Logs/GeneralLog.txt", "a+")
            self.logger.log(file, "Error while Deleting Directory : %s" % s)
            file.close()
            raise OSError

    def moveBadFilesToArchiveBad(self):

        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            source = "src/Training_Raw_files_validated/Bad_Raw/"
            if os.path.isdir(source):
                path = "src/TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.mkdir(path)
                dest = "src/TrainingArchiveBadData/BadData_" + str(date) + "_" + str(time)
                if not os.path.isdir(dest):
                    os.mkdir(dest)
                file = os.listdir(source)
                for f in file:
                    if f not in os.listdir(dest):
                        shutil.move(os.path.join(source, f), dest)
                file = open("src/Training_Logs/GeneralLog.txt", "a+")
                self.logger.log(file, "Bad files moved to archive")
                path = "src/Training_Raw_files_validated/"
                if os.path.isdir(path + "Bad_Raw/"):
                    shutil.rmtree(path + "Bad_Raw/")
                    self.logger.log(file, "Bad Raw Data Folder Deleted successfully!!")
                file.close()

        except Exception as e:
            file = open("src/Training_Logs/GeneralLog.txt", "a+")
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise CustomException(e, sys)

    def validationFileNameRaw(self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile):
        # This function validates the name of the training csv files as per given name in the schema!
        # Regex pattern is used to do the validation.If name format do not match the file is moved
        # to Bad Raw Data folder else in Good raw data.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        # create new directory
        self.createDirectoryForGoodBadRawData()
        onlyfiles = [f for f in os.listdir(self.Batch_Directory)]

        try:
            f = open("src/Training_Logs/nameValidationLog.txt", "a+")
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split(".csv", filename)
                    splitAtDot = (re.split("_", splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("src/Training_Batch_Files/" + filename,
                                        "src/Training_Raw_files_validated/Good_Raw")
                            self.logger.log(f, "Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("src/Training_Batch_Files/" + filename,
                                        "src/Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("src/Training_Batch_Files/" + filename, "src/Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("src/Training_Batch_Files/" + filename, "src/Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
            f.close()


        except Exception as e:
            f = open("src/Training_Logs/nameValidationLog.txt", "a+")
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e

    def validateColumnLength(self, NumberofColumns):
        # This function validates the number of columns in the csv files.
        # It is should be same as given in the schema file.
        # If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
        # If the column number matches, file is kept in Good Raw Data for processing.
        try:
            f = open("src/Training_Logs/columnValidationLog.txt", "a+")
            self.logger.log(f, "Column Length Validation Started!!")
            for file in os.listdir("src/Training_Raw_files_validated/Good_Raw"):
                csv = pd.read_csv("src/Training_Raw_files_validated/Good_Raw/" + file)
                if csv.shape[1] == NumberofColumns:
                    pass

                else:
                    shutil.move("src/Training_Raw_files_validated/Good_Raw/" + file, "src/Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("src/Training_Logs/columnValidationLog.txt", "a+")
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("src/Training_Logs/columnValidationLog.txt", "a+")
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()

    def validateMissingValuesInWholeColumn(self):
        # This function validates if any column in the csv file has all values missing.
        # If all the values are missing, the file is not suitable for processing.
        # SUch files are moved to bad raw data.
        try:
            f = open("src/Training_Logs/missingValuesInColumn.txt", "a+")
            self.logger.log(f, "Missing Values Validation Started!!")
            for file in os.listdir("src/Training_Raw_files_validated/Good_Raw/"):
                csv = pd.read_csv("src/Training_Raw_files_validated/Good_Raw/" + file)
                count = 0
                for column in csv:
                    if (len(csv[column]) - csv[column].count()) == len(csv[column]):
                        count += 1
                        shutil.move("src/Training_Raw_files_validated/Good_Raw/" + file,
                                    "src/Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f, "Invalid Column for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break

                if count == 0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("src/Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)



        except OSError:
            f = open("src/Training_Logs/missingValuesInColumn.txt", "a+")
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("src/Training_Logs/missingValuesInColumn.txt", "a+")
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()
