import numpy as np
import pandas as pd
from src.file_operations import file_methods
from src.components.data_preprocessing import preprocessing
from src.components.data_ingestion import data_loader_prediction
from src.application_logging import  logger
from src.components.Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation


class prediction:
    def __init__(self,path):
        self.file_object=open("src/Prediction_Logs/Prediction_Log.txt","a+")
        self.log_writer=logger.App_Logger()
        self.pred_data_val=Prediction_Data_validation(path)


    def predictionFromModel(self):
        try:
            self.pred_data_val.deletePredictionFile()# delete existing prediction file if any
            self.log_writer.log(self.file_object,'Start of Prediction')
            data_getter=data_loader_prediction.Data_Getter_Pred(self.file_object,self.log_writer)
            data=data_getter.get_data()

            preprocessor=preprocessing.Preprocessor(self.file_object,self.log_writer)
            data=preprocessor.remove_columns(data,['policy_number', 'policy_bind_date', 'policy_state', 'insured_zip',
                                                'incident_location', 'incident_date', 'incident_state', 'incident_city',
                                                'insured_hobbies', 'auto_make', 'auto_model', 'auto_year', 'age',
                                                'total_claim_amount'])#remove column as they will not contribute to prediction
            data.replace('?', np.nan, inplace=True) # replacing the '? with nan

            # check if missing values are present in the dataset
            is_null_present, cols_with_missing_values=preprocessor.is_null_present(data)

            # If the missing values are there then impute with appropriate imputer.
            if (is_null_present):
                data=preprocessor.impute_missing_values(data,cols_with_missing_values)# missing value imputation
                #data.to_csv("src.dummy1.csv", index=False)


            # encode categorical data
            data=preprocessor.encode_categorical_columns(data)



            file_loader=file_methods.File_Operation(self.file_object,self.log_writer)
            kmeans=file_loader.load_model('KMeans')

            # Creating the cluster of the test data for which prediction need to be done
            clusters=kmeans.predict(data)
            # Standard scaling of the data as
            data=preprocessor.scale_numerical_columns(data)
            #data.to_csv("src.dummy5.csv",index=False)
            data['clusters']=clusters
            clusters=data['clusters'].unique()
            predictions=[]
            #self.log_writer.log(self.file_object, 'Start of ankit pred1')

            for i in clusters:
                cluster_data= data[data['clusters']==i]
                cluster_data = cluster_data.drop(['clusters'],axis=1)
                #self.log_writer.log(self.file_object, 'Start of ankit pred mod start')

                model_name=file_loader.find_correct_model_file(i)
                model=file_loader.load_model(model_name)
                result=(model.predict(cluster_data))
                #self.log_writer.log(self.file_object, 'Start of ankit pred mod start2')
                for res in result:
                    if res==0:
                        predictions.append('N')
                    else:
                        predictions.append('Y')

            final=pd.DataFrame(list(zip(predictions)),columns=['Prediction'])
            path="src/Prediction_Output_File/Predictions.csv"
            final.to_csv(path,header=True,mode='a+')
            self.log_writer.log(self.file_object,'End of Prediction')
        except Exception as ex:
            self.log_writer.log(self.file_object,'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        return path











