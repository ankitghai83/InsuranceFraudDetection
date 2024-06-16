import pickle
import os
import shutil



class File_Operation:
    # This class will help in saving the model in training and call these model at the time of prediction
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.model_directory='src/components/models/'


    def save_model(self,model,filename):
        # This method will save the model at the directoery
        self.logger_object.log(self.file_object,'Entered the save_model method of the File_Operation class')
        try:
            path=os.path.join(self.model_directory,filename)# Create seperate directory for each cluster
            if os.path.isdir(path):# remove previously existing models for each cluster
                shutil.rmtree(path)
                os.mkdir(path)
            else:
                os.mkdir(path)
            with open(path+'/'+filename+'.sav','wb') as f:
                pickle.dump(model,f)# save the model file

            self.logger_object.log(self.file_object,'model file '+filename+'.sav saved. Exited the save_model method of the Model_Finder class')

            return 'successfully saved'

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in save_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Model File '+filename+' could not be saved. Exited the save_model method of the Model_Finder class')
            raise Exception()


    def load_model(self,filename):
        # This method is used to load the model flile .sav in memory
        self.logger_object.log(self.file_object,'Entered the load_model method of the File_Operation class')
        try:
            with open(self.model_directory + filename +'/'+filename+'.sav','rb') as f:
                self.logger_object.log(self.file_object,'model file '+filename+'.sav file loaded. Exited the load_model method of the Model_Finder class')
                return pickle.load(f)

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in load_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Model File ' + filename + ' could not be saved. Exited the load_model method of the Model_Finder class')
            raise Exception()


    def find_correct_model_file(self,cluster_number):
        # This model is used for the finding the correct model as per cluster number
        self.logger_object.log(self.file_object,'Entered the find_correct_model_file method of the File_Operation class')
        try:
            self.cluster_number=cluster_number
            self.folder_name=self.model_directory
            self.list_of_model_files=[]
            self.list_of_files=os.listdir(self.folder_name)
            for self.file in self.list_of_files:
                try:
                    if (self.file.index(str(self.cluster_number))!=-1):
                        self.model_name=self.file

                except:
                    continue

            self.model_name=self.model_name.split('.')[0]
            self.logger_object.log(self.file_object,'Exited the find_correct_model_file method of the Model_Finder class.')
            return self.model_name

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in find_correct_model_file method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Exited the find_correct_model_file method of the Model_Finder class with Failure')

            raise Exception()
