import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from src.file_operations import file_methods


class KMeansClustering:
    # This class is used to make a cluster of data before doing training
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object =logger_object

    def elbow_plot(self,data):
        # This method is used to know the # of cluster need to make out of data via elbow plot.
        self.logger_object.log(self.file_object,"Entered the elbow_plot method of the KMeansClustering class")
        wcss=[] # initialising withing cluster sum of square
        try:
            for i in range(1,11):
                kmeans=KMeans(n_clusters=i,init='k-means++',random_state=42)
                kmeans.fit(data)
                wcss.append(kmeans.inertia_)
            plt.plot(range(1,11),wcss)#creating graph between # of cluster and wcss
            plt.xlabel("Number of clusters")
            plt.title("The Elbow method")
            plt.ylabel("wcss")
            plt.savefig("src/preprocessing_data/K-Means_Elbow.PNG")# saving the elbow plot locally
            # finding required # if clusters
            self.kn=KneeLocator(range(1,11),wcss,curve='convex',direction='decreasing')
            self.logger_object.log(self.file_object,"The optimal number of cluster is: "+str(self.kn.knee)+" . Exited the elbow_plot method of the KMeansClustering class")
            return self.kn.knee

        except Exception as e:
            self.logger_object.log(self.file_object,"Exception occured in elbow_plot method of the KMeansClustering class. Exception message:  " + str(e))
            self.logger_object.log(self.file_object,"Finding the number of clusters failed. Exited the elbow_plot method of the KMeansClustering class")
            raise Exception()

    def create_clusters(self,data,number_of_clusters):
        # This method will create clusters of the given data and assign the cluster number to each records with new column created in dataframe
        self.logger_object.log(self.file_object,"Entered the create_clusters method of the KMeansClustering class")
        self.data=data
        try:
            self.kmeans=KMeans(n_clusters=number_of_clusters,init='k-means++',random_state=42)
            self.y_kmeans=self.kmeans.fit_predict(data)#Divide data into cluster
            #data.to_csv("src.data_ghai.csv", index=False)

            self.file_op=file_methods.File_Operation(self.file_object,self.logger_object)
            self.save_model=self.file_op.save_model(self.kmeans,'KMeans')
            self.data['Cluster']=self.y_kmeans
            self.logger_object.log(self.file_object,'succesfully created '+str(self.kn.knee)+ 'clusters. Exited the create_clusters method of the KMeansClustering class')
            return self.data

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in create_clusters method of the KMeansClustering class. Exception message:  ' + str(e))
            self.file_object.log(self.file_object,'Fitting the data to clusters failed. Exited the create_clusters method of the KMeansClustering class')
            raise Exception()

