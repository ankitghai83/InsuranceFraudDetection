from datetime import datetime
import pandas as pd
import os
from src.application_logging.logger import App_Logger

class dataTransform:
    #DEscription :his class shall be used for transforming the Good Raw Training Data before loading it in Database!!.


    def __init__(self):
        self.goodDataPath="src/Training_Raw_files_validated/Good_Raw"
        self.logger=App_Logger()



    def replaceMissingWithNull(self):
    # This method will add the quotes in all the string columns identified such that there will no leading and training space plus all missing value will be treated as NULL


        log_file=open("src/Training_Logs/dataTransformLog.txt","a+")
        try:
            onlyfiles=[f for f in os.listdir(self.goodDataPath)]
            for file in onlyfiles:
                data=pd.read_csv(self.goodDataPath + "/" + file)
                columns = ["policy_bind_date", "policy_state", "policy_csl", "insured_sex", "insured_education_level",
                           "insured_occupation", "insured_hobbies", "insured_relationship", "incident_state",
                           "incident_date", "incident_type", "collision_type", "incident_severity",
                           "authorities_contacted", "incident_city", "incident_location", "property_damage",
                           "police_report_available", "auto_make", "auto_model", "fraud_reported"]

                for col in columns:
                    data[col] = data[col].apply(lambda x: "'" + str(x) + "'")

                #data.fillna('NULL', inplace=True)
                # list of columns with string datatype variables
                    #columns = ["policy_bind_date", "policy_state", "policy_csl", "insured_sex", "insured_education_level",
                    #           "insured_occupation", "insured_hobbies", "insured_relationship", "incident_state",
                    #           "incident_date", "incident_type", "collision_type", "incident_severity",
                    #          "authorities_contacted", "incident_city", "incident_location", "property_damage",
                    #          "police_report_available", "auto_make", "auto_model", "fraud_reported"]
                #for col in columns:
                #    data[col] = data[col].apply(lambda x:"'"+str(x)+"'")

                data.to_csv(self.goodDataPath + "/" + file, index=None, header=True)
                self.logger.log(log_file, " %s: Quotes added successfully!!" % file)



        except Exception as e:
            self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
            log_file.close()
        log_file.close()
