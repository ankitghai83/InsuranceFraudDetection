import pandas as pd
import numpy as np
from sklearn_pandas import categorical_imputer, CategoricalImputer
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import RandomOverSampler

class Preprocessor:
    # This class is used to clean and transform the data before training

   def __init__(self,file_object,logger_object):
       self.file_object = file_object
       self.logger_object =logger_object

   def remove_unwanted_spaces(self,data):
       # THis method remove unwanted spaces from the dataframe
       self.logger_object.log(self.file_object, 'Entered the remove_unwanted_spaces method of the Preprocessor class')
       self.data=data

       try:
           self.df_without_spaces=self.data.apply(lambda x:x.str.strip() if x.dtype == 'object' else x)
           self.logger_object.log(self.file_object, 'Unwanted spaces removal Successful.Exited the remove_unwanted_spaces method of the Preprocessor class')
           return self.df_without_spaces

       except Exception as e:
           self.logger_object.log(self.file_object, 'Exception occured in remove_unwanted_spaces method of the Preprocessor class. Exception message:  ' + str(e))

           self.logger_object.log(self.file_object, 'unwanted space removal Unsuccessful. Exited the remove_unwanted_spaces method of the Preprocessor class')

       raise Exception()



   def remove_columns(self,data,columns):
       #This method removes the given columns from a pandas dataframe.
       self.logger_object.log(self.file_object, 'Entered the remove_columns method of the Preprocessor class')
       self.data=data
       self.columns=columns

       try:
           self.useful_data=self.data.drop(labels=self.columns, axis=1)
           self.logger_object.log(self.file_object, 'Column removal Successful.Exited the remove_columns method of the Preprocessor class')
           return self.useful_data
       except Exception as e:
           self.logger_object.log(self.file_object, 'Exception occured in remove_columns method of the Preprocessor class. Exception message:  '+str(e))
           self.logger_object.log(self.file_object, 'Column removal Unsuccessful. Exited the remove_columns method of the Preprocessor class')

           raise Exception()



   def separate_label_feature(self,data,label_column_name ):
       self.logger_object.log(self.file_object, 'Entered the separate_label_feature method of the Preprocessor class')
       try:
           self.X=self.data.drop(labels=label_column_name,axis=1)
           self.Y=data[label_column_name]
           self.logger_object.log(self.file_object, 'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
           return self.X,self.Y

       except Exception as e:
           self.logger_object.log(self.file_object, 'Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(e))
           self.logger_object.log(self.file_object, 'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
           raise Exception()


   def is_null_present(self,data):

       #This method is used check if there is any null value in the dataframe if there is then return True else False and list of column that
       # has null value

       self.logger_object.log(self.file_object, 'Entered the is_null_present method of the Preprocessor class')
       self.null_present = False
       self.cols_with_missing_values=[]
       self.cols=data.columns

       try:
           self.null_counts=data.isna().sum()
           for i in range(len(self.null_counts)):
               if self.null_counts[i]>0:
                   self.null_present = True
                   self.cols_with_missing_values.append(self.cols[i])

           if self.null_present:
               self.dataframe_with_null=pd.DataFrame()
               self.dataframe_with_null['columns']=data.columns
               self.dataframe_with_null['missing values count']=np.asarray(data.isna().sum())
               self.dataframe_with_null.to_csv('src/preprocessing_data/null_values.csv')# storing null value column information in file
           self.logger_object.log(self.file_object, 'Finding missing values is a success.Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
           return self.null_present,self.cols_with_missing_values


       except Exception as e:
           self.logger_object.log(self.file_object, 'Exception occured in is_null_present method of the Preprocessor class. Exception message:  ' + str(e))
           self.logger_object.log(self.file_object, 'Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
           raise Exception()

   def impute_missing_values(self,data,cols_with_missing_values):
       # This method will impute the missing value column with the categorical imputer as such column has only missing value
       self.logger_object.log(self.file_object, 'Entered the impute_missing_values method of the Preprocessor class')
       self.data=data
       self.cols_with_missing_values=cols_with_missing_values
       try:
           self.imputer=CategoricalImputer()
           for col in self.cols_with_missing_values:
               self.data[col]=self.imputer.fit_transform(self.data[col])
           self.logger_object.log(self.file_object, 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
           return self.data

       except Exception as e:
           self.logger_object.log(self.file_object,'Exception occured in impute_missing_values method of the Preprocessor class. Exception message:  ' + str(e))
           self.logger_object.log(self.file_object, 'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')
           raise Exception()

   def scale_numerical_columns(self,data):
       # This method will scaled the numerical data using Standard scalar
       self.logger_object.log(self.file_object, 'Entered the scale_numerical_columns method of the Preprocessor class')
       self.data=data
       self.num_df=self.data[['months_as_customer', 'policy_deductable', 'umbrella_limit',
                          'capital-gains', 'capital-loss', 'incident_hour_of_the_day',
                          'number_of_vehicles_involved', 'bodily_injuries', 'witnesses', 'injury_claim',
                          'property_claim',
                          'vehicle_claim']]
       try:
           self.scaler=StandardScaler()
           self.scaled_data=self.scaler.fit_transform(self.num_df)
           self.scaled_num_df=pd.DataFrame(data=self.scaled_data,columns=self.num_df.columns,index=self.data.index)
           self.data.drop(columns=self.scaled_num_df.columns,inplace=True)
           self.data=pd.concat([self.scaled_num_df, self.data],axis=1)

           self.logger_object.log(self.file_object, 'scaling for numerical values successful. Exited the scale_numerical_columns method of the Preprocessor class')

           return self.data

       except Exception as e:
           self.logger_object.log(self.file_object, 'Exception occured in scale_numerical_columns method of the Preprocessor class. Exception message:  ' + str(e))
           self.logger_object.log(self.file_object, ' scaling for numerical columns Failed. Exited the scale_numerical_columns method of the Preprocessor class')

           raise Exception()

   def encode_categorical_columns(self,data):
       # This methoD will encode/map the categorical columns with numeric code as model work on numberical data

      self.logger_object.log(self.file_object, 'Entered the encode_categorical_columns method of the Preprocessor class')
      self.data=data

      try:
          self.cat_df=self.data.select_dtypes(include=['object']).copy()
          self.cat_df['policy_csl']=self.cat_df['policy_csl'].map({'100/300': 1, '250/500': 2.5, '500/1000': 5})
          self.cat_df['insured_education_level']=self.cat_df['insured_education_level'].map({'JD': 1, 'High School': 2, 'College': 3, 'Masters': 4, 'Associate': 5, 'MD': 6, 'PhD': 7})
          self.cat_df['incident_severity']=self.cat_df['incident_severity'].map({'Trivial Damage': 1, 'Minor Damage': 2, 'Major Damage': 3, 'Total Loss': 4})
          self.cat_df['insured_sex']=self.cat_df['insured_sex'].map({'FEMALE': 0, 'MALE': 1})
          self.cat_df['property_damage']=self.cat_df['property_damage'].map({'NO': 0, 'YES': 1})
          self.cat_df['police_report_available']=self.cat_df['police_report_available'].map({'NO': 0, 'YES': 1})
          try:
              # This block will run when there is training of model will run
              self.cat_df['fraud_reported']=self.cat_df['fraud_reported'].map({'N': 0, 'Y': 1})
              self.cols_to_drop=['policy_csl', 'insured_education_level', 'incident_severity', 'insured_sex',
                                            'property_damage', 'police_report_available', 'fraud_reported']

          except :
              # This block will run when there is prediction of test data
              self.cols_to_drop=['policy_csl', 'insured_education_level', 'incident_severity', 'insured_sex',
                                            'property_damage', 'police_report_available']

          # Using the dummy encoding to encode the categorical columns to numerical ones
          for col in self.cat_df.drop(columns=self.cols_to_drop).columns:
              self.cat_df=pd.get_dummies(self.cat_df, columns=[col], prefix=[col], drop_first=True)

          self.data.drop(columns=self.data.select_dtypes(include=['object']).columns, inplace=True)
          self.data=pd.concat([self.cat_df,self.data],axis=1)
          self.logger_object.log(self.file_object,'encoding for categorical values successful. Exited the encode_categorical_columns method of the Preprocessor class')
          return self.data

      except Exception as e:
          self.logger_object.log(self.file_object,'Exception occured in encode_categorical_columns method of the Preprocessor class. Exception message:  ' + str(e))
          self.logger_object.log(self.file_object,'encoding for categorical columns Failed. Exited the encode_categorical_columns method of the Preprocessor class')
          raise Exception()


   def handle_imbalanced_dataset(self,x,y):
       # This method can handle imbalanced dataset and do over or under or smot sampling to make it balanced
       self.logger_object.log(self.file_object,'Entered the handle_imbalanced_dataset method of the Preprocessor class')

       try:
           self.rdsmple=RandomOverSampler()
           self.x_sampled,self.y_sampled=self.rdsmple.fit_resample(x,y)
           self.logger_object.log(self.file_object,'dataset balancing successful. Exited the handle_imbalanced_dataset method of the Preprocessor class')

           return self.x_sampled,self.y_sampled
       except Exception as e:
           self.logger_object.log(self.file_object,'Exception occured in handle_imbalanced_dataset method of the Preprocessor class. Exception message:  ' + str(e))
           self.logger_object.log(self.file_object,'dataset balancing Failed. Exited the handle_imbalanced_dataset method of the Preprocessor class')
           raise Exception()














