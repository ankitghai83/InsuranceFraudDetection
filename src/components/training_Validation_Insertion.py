from datetime import datetime
from src.components.Training_Raw_data_validation.rawValidation import Raw_Data_validation
from src.components.DataTransform_Training.DataTransformation import dataTransform
from src.components.DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from src.application_logging import logger

class train_validation:
    def __init__(self,path):
        self.raw_data=Raw_Data_validation(path)
        self.dataTransform=dataTransform()
        self.dBOperation=dBOperation()
        self.file_object=open("src\Training_Logs\Training_Main_Log.txt","a+")
        self.log_writer=logger.App_Logger()


    def train_validation(self):
        try:
            self.log_writer.log(self.file_object,"Start of Validation on files!!")
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns=self.raw_data.valueFromSchema()
            # getting the regex defined to validate filename
            regex=self.raw_data.manualRegexCreation()
            #validating the file name of the training batch files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            # Validation of # of columns in the client batch file
            self.raw_data.validateColumnLength(noofcolumns)
            # Validating if there is missing value in any of the column of the client batch file.
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log(self.file_object,"Raw Data Validation Complete!!")
            self.log_writer.log(self.file_object,"Starting Data Transforamtion!!")
            #replacing the missing value with NULL
            self.dataTransform.replaceMissingWithNull()
            self.log_writer.log(self.file_object,"DataTransformation Completed!!!")
            self.log_writer.log(self.file_object,"Creating Training_Database and tables on the basis of given schema!!!")

            # Creating database with given name, if present open the connection! Create table with columns given in schema

            self.dBOperation.createTableDb('Training',column_names)
            self.log_writer.log(self.file_object,"Table creation Completed!!")

            #Inserting the columns in the table created

            self.log_writer.log(self.file_object,"Insertion of Data into Table started!!!!")
            self.dBOperation.insertIntoTableGoodData('Training')

            self.log_writer.log(self.file_object,"Insertion in Table completed!!!")
            # Deleting the Good data folder as the data insertion completed

            self.log_writer.log(self.file_object,"Deleting Good Data Folder!!!")
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.log_writer.log(self.file_object,"Good_Data folder deleted!!!")
            self.log_writer.log(self.file_object,"Moving bad files to Archive and deleting Bad_Data folder!!!")

            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(self.file_object,"Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.file_object,"Validation Operation completed!!")

            self.log_writer.log(self.file_object,"Extracting csv file from table")

            self.dBOperation.selectingDatafromtableintocsv('Training')
            self.file_object.close()

        except Exception as e:
            raise e
