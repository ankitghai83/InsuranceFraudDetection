"""
This is the Entry point for Training the Machine Learning Model.
"""
from sklearn.cluster import KMeans
# Doing necessary imports
from sklearn.model_selection import train_test_split
from src.components.data_ingestion import data_loader
from src.components.data_preprocessing import preprocessing
from src.components.data_preprocessing import clustering
from src.file_operations import file_methods
from src.components.best_model_finder import tuner
from src.application_logging import logger
import numpy as np
import pandas as pd


class trainModel:
    def __init__(self):
        self.log_writer = logger.App_Logger()
        self.file_object = open('src/Training_Logs/ModelTrainingLog.txt', 'a+')

    def trainingModel(self):
        # logging start of the training
        self.log_writer.log(self.file_object, 'Start of Training')
        try:
            # Getting data from the source
            data_getter = data_loader.Data_Getter(self.file_object, self.log_writer)
            data = data_getter.get_data()

            """doing the data preprocessing"""
            preprocessor = preprocessing.Preprocessor(self.file_object, self.log_writer)
            data = preprocessor.remove_columns(data,
                                               ['policy_number', 'policy_bind_date', 'policy_state', 'insured_zip',
                                                'incident_location', 'incident_date', 'incident_state', 'incident_city',
                                                'insured_hobbies', 'auto_make', 'auto_model', 'auto_year', 'age',
                                                'total_claim_amount'])  # remove the column as it doesn't contribute to prediction.
            data.replace('?', np.NaN, inplace=True)  # replacing ? with Nan values for imputation

            # check if null value is present in the dataset
            is_null_present, cols_with_missing_values = preprocessor.is_null_present(data)

            # if missing values are there then we will do appropriate imputation
            if is_null_present:
                data = preprocessor.impute_missing_values(data, cols_with_missing_values)  # impute missing values


            # Encode the categorical data
            data = preprocessor.encode_categorical_columns(data)


            # create seperate features and label
            X, Y = preprocessor.separate_label_feature(data, label_column_name='fraud_reported')

            """ Applying the clustering approach"""

            kmeans = clustering.KMeansClustering(self.file_object, self.log_writer)  # object initiation
            number_of_clusters = kmeans.elbow_plot(X)  # using the elbow plot to find the number of optimum clusters
            # Divide the data into clusters
            X = kmeans.create_clusters(X, number_of_clusters)

            # create a new column in the dataset consisting of the corresponding cluster assignments.
            X['Labels'] = Y

            # getting the unique clusters from our dataset

            list_of_clusters = X['Cluster'].unique()

            """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""

            for i in list_of_clusters:
                cluster_data = X[X['Cluster'] == i]  ## filter data for ith cluster

                # Prepare the feature and Label columns
                cluster_features = cluster_data.drop(['Labels', 'Cluster'], axis=1)
                cluster_label = cluster_data['Labels']

                # splitting the data into training and test set for each cluster one by one
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1 / 3,
                                                                    random_state=355)

                # Doing normalisation on the train & test feature columns though it is a part of data pre processing but to avoid data
                # leakage we do it after split
                x_train = preprocessor.scale_numerical_columns(x_train)
                x_test = preprocessor.scale_numerical_columns(x_test)

                model_finder = tuner.Model_Finder(self.file_object,self.log_writer)  # Object initilaisation

                # getting best model for each of the cluster
                best_model_name, best_model = model_finder.get_best_model(x_train, y_train, x_test, y_test)

                # saving the model in the models directory as per eacgh cluster
                file_op = file_methods.File_Operation(self.file_object, self.log_writer)
                save_model=file_op.save_model(best_model,best_model_name+str(i))

            # logging the successful Training
            self.log_writer.log(self.file_object, 'Successful End of Training')
            self.file_object.close()



        except Exception as e:
            self.log_writer.log(self.file_object, 'Unsuccessful End of Training')
            self.file_object.close()
            raise Exception()
