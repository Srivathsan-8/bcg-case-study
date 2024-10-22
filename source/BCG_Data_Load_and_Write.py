import configparser
import sys
from source.logger import LogCapture

class DataPlay():

    def __init__(self,spark):
        self.spark = spark
        self.APP_CONFIG_PATH = "/Workspace/Users/vkksrivathsan@gmail.com/bcg-case-study/config.cfg"
        self.APP_DETAILS = "BCG_CASE_STUDY"
        self.logg=LogCapture()

    def get_app_conf_dict(self):
        """Creating this method for parsing the config file as a dictionary to get the paths respective to the use case and the file.
        """
        try:
            config = configparser.RawConfigParser()
            config.read(self.APP_CONFIG_PATH) 
            config_dict = dict(config.items(self.APP_DETAILS))  
            return config_dict
        except Exception as err:
            print(f"Error: {err}")
            self.logg.log_error(f'Error: {err}')
            raise

    def file_to_df(self, input_file):
    
        """Based on the input file to the method, this will call the config method to get the path and creates a spark dataframe from this and returns the df if called.
        """
        try:
            config_dict = self.get_app_conf_dict()  # Get the configuration dictionary
            df_path = config_dict[input_file]  # Retrieve the file path from the config
            df = self.spark.read.csv(df_path, inferSchema=True, header=True, sep=",")
            # Read the CSV as a DataFrame
            self.logg.log_success(f"Created the DataFrame Successfully for the input: {input_file}")
            return df
        except Exception as err:
            print(f"Error reading file: {err}")
            self.logg.log_error(f"Error in creating the DataFrame for the input: {input_file}")
            raise
    
    def df_to_file(self,write_df,f_name):

        """If this method is called it get's the target path and writes the passed dataframe to the passed filename and stores as csv to the target path.
        """
        try:
            config_dict = self.get_app_conf_dict()
            target = config_dict["output_path"]
            target_path=f"{target}/{f_name}"
            write_df.coalesce(1).write.mode("overwrite").csv(target_path,header=True)
            self.logg.log_success(f"Successfully Stored the Analysis in the mentioned path: {target_path}")
        except Exception as err:
            print(f"Error reading file: {err}")
            self.logg.log_error(f"Error in Storing the file: {err}")
            raise           
