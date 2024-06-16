import pandas as pd


class Data_Getter_Pred:
    # This class will be used to fetch data recieved for prediction
    def __init__(self,file_object, logger_object):
        self.file_object=file_object
        self.logger_object=logger_object
        self.prediction_file="src/Prediction_FileFromDB/InputFile.csv"

    def get_data(self):
        #This method reads the data from source.
        self.logger_object.log(self.file_object,'Entered the get_data method of the Data_Getter class')
        try:
            self.data=pd.read_csv(self.prediction_file)
            self.logger_object.log(self.file_object,'Data Load Successful.Exited the get_data method of the Data_Getter class')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_data method of the Data_Getter class. Exception message: '+str(e))
            self.logger_object.log(self.file_object,'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise Exception()


