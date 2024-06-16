from sklearn.svm import SVC
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, accuracy_score


class Model_Finder:
    # This class shall be used to find the model with best accuracy and AUC score
    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.sv_classifier = SVC()
        self.xgb = XGBClassifier(objective='binary:logistic', n_jobs=-1)

    def get_best_params_for_svm(self, train_x, train_y):
        # Get the best parameter for the SVM model for better accuracy using GridSearchCV
        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_svm method of the Model_Finder class')
        try:
            # initializing with different combination of parameters
            self.param_grid = {"kernel": ['rbf', 'sigmoid'], "C": [0.1, 0.5, 1.0], "random_state": [0, 100, 200, 300]}
            # creating an object of GridSearch Class
            self.grid = GridSearchCV(estimator=self.sv_classifier, param_grid=self.param_grid, cv=5, verbose=3)
            # finding the best parameter
            self.grid.fit(train_x, train_y)

            # extracting the best parameters
            self.kernel = self.grid.best_params_['kernel']
            self.random_state = self.grid.best_params_['random_state']
            self.C = self.grid.best_params_['C']

            # creating a new model with the best parameters
            self.sv_classifier = SVC(kernel=self.kernel, random_state=self.random_state, C=self.C)
            self.sv_classifier.fit(train_x, train_y)

            self.logger_object.log(self.file_object, 'SVM best params: ' + str(self.grid.best_params_) + '. Exited the get_best_params_for_svm method of the Model_Finder class')

            return self.sv_classifier

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in get_best_params_for_svm method of the Model_Finder class. Exception message:  ' + str(
                                       e))

            self.logger_object.log(self.file_object,
                                   'SVM training  failed. Exited the get_best_params_for_svm method of the Model_Finder class')

    def get_best_params_for_xgboost(self, train_x, train_y):
        # get the best parameter for XGBoost algorithm that get best accuracy using GridSearch CV

        self.logger_object.log(self.file_object,
                               'Entered the get_best_params_for_xgboost method of the Model_Finder class')
        # Initialize the list parameter that will be evaluated to find best possible param
        try:

            self.param_grid_xgboost = {"n_estimators": [100, 130], "criterion": ['gini', 'entropy'],
                                       "max_depth": range(8, 10, 1)}

            # creating the object of GridSearch CV

            self.grid = GridSearchCV(self.xgb, param_grid=self.param_grid_xgboost, cv=5, verbose=3)
            # finding the best parameter
            self.grid.fit(train_x, train_y)

            # extracting the best parameters
            self.n_estimators = self.grid.best_params_['n_estimators']
            self.criterion = self.grid.best_params_['criterion']
            self.max_depth = self.grid.best_params_['max_depth']

            # creating a new model with the best parameters
            self.xgb = XGBClassifier(n_estimators=self.n_estimators, criterion=self.criterion, max_depth=self.max_depth,
                                     n_job=-1)

            # training new model
            self.xgb.fit(train_x, train_y)
            self.logger_object.log(self.file_object, 'XGBoost best params: ' + str(
                self.grid.best_params_) + '. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            return self.xgb
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in get_best_params_for_xgboost method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'XGBoost Parameter tuning  failed. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            raise Exception()

    def get_best_model(self, train_x, train_y, test_x, test_y):
        # This method is used to find the best model based on AUC-ROC score or accuracy metrics

        self.logger_object.log(self.file_object, 'Entered the get_best_model method of the Model_Finder class')
        try:
            self.xgboost = self.get_best_params_for_xgboost(train_x, train_y)
            self.prediction_xgboost = self.xgboost.predict(test_x)  # Prediction using xgboost model

            if len(test_y.unique()) == 1:  # if the length =1 then its 1 label in y , then auc_roc_score return error and we should use accuracy metrics
                self.xgboost_score = accuracy_score(test_y, self.prediction_xgboost)
                self.logger_object.log(self.file_object,'Accuracy for XGBoost:' + str(self.xgboost_score))  # log accuracy for xgboost
            else:
                self.xgboost_score = roc_auc_score(test_y, self.prediction_xgboost)
                self.logger_object.log(self.file_object,'AUC for XGBoost:' + str(self.xgboost_score))  # lof auc_roc score of the xgboost

            self.svm = self.get_best_params_for_svm(train_x, train_y)
            self.prediction_svm = self.svm.predict(test_x)  # Prediction using the svm model

            if len(test_y.unique()) == 1:
                self.svm_score = accuracy_score(test_y, self.prediction_svm)
                self.logger_object.log(self.file_object,'Accuracy for SVM:' + str(self.svm_score))  # checking accuracy of the model

            else:
                self.svm_score = roc_auc_score(test_y, self.prediction_svm)
                self.logger_object.log(self.file_object,'AUC for SVM:' + str(self.svm_score))  # checking auc_roc score of the model

            # Comparing score of 2 model to find best model
            if (self.svm_score < self.xgboost_score):
                return 'XGBoost',self.xgboost
            else:
                return 'SVM', self.sv_classifier

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_best_model method of the Model_Finder class. Exception message:  ' + str(e))

            self.logger_object.log(self.file_object,'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')

            raise Exception()
