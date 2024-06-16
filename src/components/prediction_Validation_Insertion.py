from datetime import datetime
from src.components.Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
from src.components.DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import dBOperation
from src.components.DataTransformation_Prediction.DataTransformationPrediction import dataTransformPredict
from src.application_logging import logger

class pred_validation:
    def __init__(self,path):
        self.raw_data = Prediction_Data_validation(path)
        self.dataTransform=dataTransformPredict()
        self.dBOperation=dBOperation()
        self.file_object=open("src/Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer=logger.App_Logger()

    def prediction_validation(self):
        try:
            self.log_writer.log(self.file_object,"Start of Validation on files for prediction!!")
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns=self.raw_data.valuesFromSchema()
            #getting regex defined for the filename validation
            regex=self.raw_data.manualRegexCreation()
            # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            # validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            #validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log(self.file_object,"Raw Data Validation Complete!!")

            self.log_writer.log(self.file_object,"Starting Data Transforamtion!!")
            #replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()
            self.log_writer.log(self.file_object,"DataTransformation Completed!!!")

            self.log_writer.log(self.file_object,"Creating Prediction_Database and tables on the basis of given schema!!!")
            self.dBOperation.createTableDb('Prediction',column_names)
            self.log_writer.log(self.file_object,"Table creation Completed!!")

            self.log_writer.log(self.file_object,"Insertion of Data into Table started!!!!")
            # insert csv files in the table
            self.dBOperation.insertIntoTableGoodData('Prediction')
            self.log_writer.log(self.file_object,"Insertion in Table completed!!!")
            self.log_writer.log(self.file_object,"Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataPredictionFolder()
            self.log_writer.log(self.file_object,"Good_Data folder deleted!!!")
            self.log_writer.log(self.file_object,"Moving bad files to Archive and deleting Bad_Data folder!!!")
            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(self.file_object,"Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.file_object,"Validation Operation completed!!")
            self.log_writer.log(self.file_object,"Extracting csv file from table")
            self.dBOperation.selectingDatafromtableintocsv('Prediction')

        except Exception as e:
            raise e