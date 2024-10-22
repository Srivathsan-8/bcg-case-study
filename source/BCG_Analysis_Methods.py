from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql.functions import *
from source.logger import LogCapture
from source.BCG_Data_Load_and_Write import DataPlay

"""Class BCG Analysis contains 10 methods - Each for the given 10 analysis
We will be passing the spark session from the Main script while calling the class.
"""
class BCGAnalysis:
    def __init__(self, spark):

        """ We are initialising the three dataframes here with None and each method is having an if condition to check if it's an empty df or not for the required files and it reads the file only when a new object is created, if all the methods are called with the same object it prevents multiple loading of the file to df which is redundant.
        """

        self.spark = spark  # Initialize the Spark session
        self.log = LogCapture()  # Initialize logging
        self.person_df = None
        self.units_df = None
        self.damage_df = None
        self.file_write_obj = DataPlay(self.spark)

    def male_accidents(self):
        """ Method to Find the number of crashes (accidents) in which number of males killed are greater than 2
        Created the Person Dataframe by calling the DataPlay Class by passing the config key for the file path.
        Filtering the person_df with the gender as male and then using group by on the sum on Death Count column for better performance
        Also repartitioned the dataframe on CRASH_ID Column as it has less Standard Deviation than Mean which implies
        less varied records in CRASH_ID.
        Returning the count back to main python script and also capturing it in the log file by calling the LogCapture Class.
        """
        
        try:
            if self.person_df is None:
                person_obj = DataPlay(self.spark)
                self.person_df = person_obj.file_to_df("person_path")
            
            male_person_df = self.person_df.filter(col("PRSN_GNDR_ID") == 'MALE')
            count_male_acc = male_person_df.repartition(col("CRASH_ID")).groupBy(col("CRASH_ID")) \
                .agg(sum(col("DEATH_CNT")).alias("Male_Death_Count")) \
                .filter(col("Male_Death_Count") > 2).count()

            self.log.log_success(f"Successfully extracted the first analysis. "
                                 f"The count of crashes with at least 2 males dead is: {count_male_acc}")

            return {"status": "success", "Count": count_male_acc, "Analysis": "Male_Accidents_more_than_2_cnt"}
        
        except Exception as err:
            self.log.log_error(f"Error during male accident analysis: {err}")
            return {"status": "error", "message": str(err)}
        
    def two_wheeler_accidents(self):

        """ Method to find the total two wheelers which are booked for crashes
        Created the Units Dataframe by calling the DataPlay Class by passing the config key for the file path.
        Also repartitioned the dataframe on CRASH_ID Column as it has less Standard Deviation than Mean which implies
        less varied records in CRASH_ID.
        Filtered the dataframe on VEH_BODY_STYL_ID for MOTORCYCLE and storing the count and 
        Returning the count back to main python script and also capturing it in the log file by calling the LogCapture Class.
        """

        try:
            if self.units_df is None:
                unit_obj = DataPlay(self.spark)
                self.units_df = unit_obj.file_to_df("units_path")
            
            two_wheeler_cnt = self.units_df.repartition(col("CRASH_ID")) \
                .filter(col("VEH_BODY_STYL_ID") == "MOTORCYCLE").count()

            self.log.log_success(f"Successfully extracted the second analysis. "
                                 f"The count of crashes involving two-wheeler is: {two_wheeler_cnt}")

            return {"status": "success", "Count": two_wheeler_cnt, "Analysis": "Two_Wheeler_Accidents_cnt"}

        except Exception as err:
            self.log.log_error(f"Error during two-wheeler accidents analysis: {err}")
            return {"status": "error", "message": str(err)}

    def vehicle_airbagnotdeployed_killed(self):

        """ Method to find the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
        From the primary_person df we are filtering where the driver died and airbages did not deploy based on the column and
        storing it in a intermediate df.
        Then we are joining with the units df on CRASH_ID Column and grouping by on VEH_MAKE_ID Column and CRASH_ID to get the top 5 makes.
        Once we get the makes we are calling the Data Read Class's df_to_file method and writing the df to a csv file and storing it into the config mentioned output path with the preferred naming.
        Filtering before Joining reduces record count and enhances performance.
        """
        try:
            if self.person_df is None:
                person_obj = DataPlay(self.spark)
                self.person_df = person_obj.file_to_df("person_path")
            
            if self.units_df is None:
                unit_obj = DataPlay(self.spark)
                self.units_df = unit_obj.file_to_df("units_path")
            
            driver_death_filtered_df = self.person_df.filter(
                (col("PRSN_TYPE_ID") == "DRIVER") &
                (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED") &
                ((col("PRSN_INJRY_SEV_ID") == "KILLED") | (col("DEATH_CNT") != 0))
            )
            
            vehicle_air_killed_df = self.units_df.repartition(self.units_df.CRASH_ID) \
                .join(broadcast(driver_death_filtered_df), on="CRASH_ID", how="inner") \
                .filter((self.units_df.DEATH_CNT != 0) & (self.units_df.VEH_MAKE_ID != "NA")) \
                .groupby(self.units_df.CRASH_ID, self.units_df.VEH_MAKE_ID) \
                .count() \
                .groupby(self.units_df.VEH_MAKE_ID) \
                .agg(count(self.units_df.VEH_MAKE_ID).alias("Vehicle_Count")) \
                .orderBy(col("Vehicle_Count").desc()) \
                .limit(5) \
                .drop(col("Vehicle_Count"))
            
            self.file_write_obj.df_to_file(vehicle_air_killed_df, "Top_Vehicle_Makes_AB_Not_Deployed.csv")

            self.log.log_success(f"Successfully extracted the third analysis and stored in the file.")
            return {"status": "success", "file_name": "Top_Vehicle_Makes_AB_Not_Deployed","Analysis":"Top 5 Vehicle Makes with Airbags Not Deployed and Person Killed"}

        except Exception as err:
            self.log.log_error(f"Error during vehicle airbag analysis: {err}")
            return {"status": "error", "message": str(err)}
        
    
    def hit_and_run_valid_df(self):

        """Method to Determine the number of Vehicles with driver having valid licences involved in hit and run  
        From the primary_person df we are filtering and selecting the CRASH_ID and UNIT_NBR where the driverhas any of the filter condition licences and storing it in a intermediate df.
        Then we are joining with the units df on CRASH_ID Column and UNIT_NBR and filtering whether the VEH_HNR_FL column which
        is flag column for hit and run case is having Y
        Once we get the makes we are calling the Data Read Class's df_to_file method
        Returning the count back to main python script and also capturing it in the log file by calling the LogCapture Class.
        """

        try:
            if self.person_df is None:
                person_obj = DataPlay(self.spark)
                self.person_df = person_obj.file_to_df("person_path")
            
            if self.units_df is None:
                unit_obj = DataPlay(self.spark)
                self.units_df = unit_obj.file_to_df("units_path")
            
            valid_lic_df=self.person_df.select(self.person_df.CRASH_ID,self.person_df.UNIT_NBR).filter(col("DRVR_LIC_TYPE_ID").isin(["COMMERCIAL DRIVER LIC.","OCCUPATIONAL","DRIVER LICENSE"]))
            
            hit_and_run_cnt=self.units_df.join(valid_lic_df,on=["CRASH_ID","UNIT_NBR"],how="INNER").filter(self.units_df.VEH_HNR_FL=="Y").count()
            
            
            self.log.log_success(f"Successfully extracted the fourth analysis. "
                                 f"The count of hit and run with valid licenses: {hit_and_run_cnt}")

            return {"status": "success", "Count": hit_and_run_cnt,"Analysis": "Hit&Run_With_License_cnt"}

        except Exception as err:
            self.log.log_error(f"Error during hit and run analysis: {err}")
            return {"status": "error", "message": str(err)}    
          
          
    
    def female_not_involved_state(self):

        """Method to determine the state that has highest number of accidents in which females were not involved. We are involving only the Person_df here and filtering based on the Gender Column and grouping by on the Driver License State
        in the same table not the Vehicle License State in the Units df as the requirement is to find based on the gender.
        Storing the state and returning it to the main script.
        """
        try:
            if self.person_df is None:
                person_obj = DataPlay(self.spark)
                self.person_df = person_obj.file_to_df("person_path")
            
            if self.units_df is None:
                unit_obj = DataPlay(self.spark)
                self.units_df = unit_obj.file_to_df("units_path")
            
            female_not_inv_df=self.person_df.repartition(self.person_df.CRASH_ID).filter((col("PRSN_GNDR_ID")!="FEMALE") & (~col("DRVR_LIC_STATE_ID").isin(["NA","Unknown","Other"]))).groupby(col("DRVR_LIC_STATE_ID")).count().orderBy(col("count").desc()).limit(1).drop(col("count"))
            
            
            self.file_write_obj.df_to_file(female_not_inv_df, "Female_Not_Involved_State.csv")

            self.log.log_success(f"Successfully extracted the fifth analysis and stored in the file.")
            return {"status": "success", "file_name": "Female_Not_Involved_State","Analysis":"Top State where Females where not involved"}

        except Exception as err:
            self.log.log_error(f"Error during female_not_involved_state analysis: {err}")
            return {"status": "error", "message": str(err)}

    def third_to_fifth_injury_model(self):

        """ Method to find the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death.
        From the units_df we are summing the TOT_INJURY_COUNT Column and the Death Count Column and storing it in a new column by ordering it in descending order.
        Then by using window function we are partitioning the complete df based on the values in the sum column.
        After that we are using dense_ranl function to rank each record to get the veh make even if the sum value is same for 2 rows. Based on the filter condition mentioned we will get the top 3 to 5 veh make id's in a dataframe and
        Once we get the makes we are calling the Data Read Class's df_to_file method and writing the df to a csv file and storing it into the config mentioned output path with the preferred naming.
        """
        try:
            if self.person_df is None:
                person_obj = DataPlay(self.spark)
                self.person_df = person_obj.file_to_df("person_path")
            
            if self.units_df is None:
                unit_obj = DataPlay(self.spark)
                self.units_df = unit_obj.file_to_df("units_path")
            
            top_vehicle_pre_inj=self.units_df.repartition(self.units_df.CRASH_ID).withColumn("Death_And_Inj_Count",col("TOT_INJRY_CNT")+col("DEATH_CNT")).orderBy(col("Death_And_Inj_Count").desc()).distinct()
        
            win=Window.orderBy(top_vehicle_inj.Death_And_Inj_Count.desc())
        
            top_vehicle_inj=top_vehicle_pre_inj.withColumn("rank",dense_rank().over(win)).filter(col("rank").isin([3,4,5])).select(col("VEH_MAKE_ID"))
            
            
            self.file_write_obj.df_to_file(top_vehicle_inj, "Top_3_5_Inj_Death_Model.csv")

            self.log.log_success(f"Successfully extracted the sixth analysis and stored in the file.")
            return {"status": "success", "file_name": "Top_3_5_Inj_Death_Model","Analysis":"Top 3_5 Vehicle Makes Contributing to Highest Injury with Death"}

        except Exception as err:
            self.log.log_error(f"Error during third_to_fifth_injury_model analysis: {err}")
            return {"status": "error", "message": str(err)}

    def ethnic_user_make_id(self):

        """ Method to determine the top ethnic user group of each unique body style in the crash.
        We are joining the units_df and person_df on broadcast join for better performance.
        From the joined df we are filtering out the incorrect data and grouping by both the PRSN_ETHNICITY_ID
        and VEH_BODY_STYL_ID columns and getting the count.
        After this we are using window function using partition on the VEH_BODY_STYL_ID column and ordering it based on the count column.
        From the units_df we are summing the TOT_INJURY_COUNT Column and the Death Count Column and storing it in a new column by ordering it in descending order.
        Then using the intermediate df we are using row number function to get the top ethnicity for each of the VEH_BODY_STYL_ID and storing it in the final df and Once we get the ethnicity we are calling the Data Read Class's df_to_file method and writing the df to a csv file and storing it into the config mentioned output path with the preferred naming.
        """
        try:
            if self.person_df is None:
                person_obj = DataPlay(self.spark)
                self.person_df = person_obj.file_to_df("person_path")
            
            if self.units_df is None:
                unit_obj = DataPlay(self.spark)
                self.units_df = unit_obj.file_to_df("units_path")
                
            unit_person_df=self.units_df.join(broadcast(self.person_df),on=["CRASH_ID","UNIT_NBR"],how="inner").distinct()
            
            veh_bod_ethnicty=unit_person_df.filter((~col("PRSN_ETHNICITY_ID").isin(["NA","UNKNOWN","OTHER"])) & (~col("VEH_BODY_STYL_ID").isin(["NA","UNKNOWN"]))).groupBy(col("VEH_BODY_STYL_ID"),col("PRSN_ETHNICITY_ID")).count()

            win_ethnicty=Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
            
            veh_bod_ethnicty_df=veh_bod_ethnicty.withColumn("rn",row_number().over(win_ethnicty)).filter(col("rn")==1).drop(col("rn"),col("count"))
        
            
            self.file_write_obj.df_to_file(veh_bod_ethnicty_df, "Top_Ethnicity_Vehicle_Body.csv")

            self.log.log_success(f"Successfully extracted the seventh analysis and stored in the file.")
            return {"status": "success", "file_name": "Top_Ethnicity_Vehicle_Body","Analysis":"Top Ethnicity for Each Vehicle Body"}
        except Exception as err:
            self.log.log_error(f"Error during ethnic_user_make_id analysis: {err}")
            return {"status": "error", "message": str(err)}
            
    def alc_driver_zip_code(self):

        """ Method to determine the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash.
        We are joining the units_df and person_df on broadcast join for better performance.
        From the joined df we are filtering based on the three contributing factors in or conditions and also using an and condition on the PRSN_ALCOHOL_TEST indicator column as well for adding a layer to the analysis and grouping by on the Driver Zip Code Column to get the zip codes with the highest count and storing it into a df and Once we get the zip codes we are calling the Data Read Class's df_to_file method and writing the df to a csv file and storing it into the config mentioned output path with the preferred naming.
        """
        try:
            if self.person_df is None:
                person_obj = DataPlay(self.spark)
                self.person_df = person_obj.file_to_df("person_path")
            
            if self.units_df is None:
                unit_obj = DataPlay(self.spark)
                self.units_df = unit_obj.file_to_df("units_path")
                
            unit_person_df=self.units_df.join(broadcast(self.person_df),on=["CRASH_ID","UNIT_NBR"],how="inner").distinct()
            
                
            alc_driver_zip_df=unit_person_df.filter(((col("PRSN_ALC_RSLT_ID").isin(["Positive"])) | (col("CONTRIB_FACTR_1_ID").isin(["UNDER INFLUENCE - ALCOHOL", "HAD BEEN DRINKING"])) | (col("CONTRIB_FACTR_2_ID").isin(["UNDER INFLUENCE - ALCOHOL", "HAD BEEN DRINKING"])) | (col("CONTRIB_FACTR_P1_ID").isin(["UNDER INFLUENCE - ALCOHOL", "HAD BEEN DRINKING"]))) & (col("DRVR_ZIP").isNotNull())).groupBy(col("DRVR_ZIP")).count().orderBy(col("count").desc()).limit(5).drop(col("count"))
            

            
            self.file_write_obj.df_to_file(alc_driver_zip_df, "Zip_Code_Drunk_Drive.csv")

            self.log.log_success(f"Successfully extracted the eight analysis and stored in the file.")
            return {"status": "success", "file_name": "Zip_Code_Drunk_Drive","Analysis":"Top 5 Zip Codes with Crash Contributing to Alcohol"}

        except Exception as err:
            self.log.log_error(f"Error during alc_driver_zip_code analysis: {err}")
            return {"status": "error", "message": str(err)}
            

    def none_damage_rating_4_distinct_crash(self):

        """ Method to determine the Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        We are reading the damages file and storing it into a df and filtering out based on where there were no damages.
        From the units_df we are filtering out the invalid data from the VEH_DMAG_SCL_1_ID and VEH_DMAG_SCL_2_ID columns and also getting records where there contains the substring INSURANCE implying there is a valid insurance for the vehicle/driver from the FIN_RESP_TYPE_ID columns. After that we are using regex function to extract the severity number of the damage from the string of the Damage Severity columns and storing it into a column and filtering based on the value present, after that we are joining with the damages_df and getting the distinct count of crash id's. Storing the count and returning to the main script.
        """
        try:
            if self.person_df is None:
                person_obj = DataPlay(self.spark)
                self.person_df = person_obj.file_to_df("person_path")
            
            if self.units_df is None:
                unit_obj = DataPlay(self.spark)
                self.units_df = unit_obj.file_to_df("units_path")
                
            if self.damage_df is None:
                damage_obj = DataPlay(self.spark)
                self.damage_df = damage_obj.file_to_df("damages_path")
                
            none_damage_df=self.damage_df.filter(col("DAMAGED_PROPERTY").like("%NONE%")).distinct()
            
            unit_insured_max_sev_pre_df=self.units_df.filter(((~col("VEH_DMAG_SCL_1_ID").isin(["INVALID VALUE","NA","NO DAMAGE"])) | (~col("VEH_DMAG_SCL_2_ID").isin(["INVALID VALUE","NA","NO DAMAGE"]))) & (col("FIN_RESP_TYPE_ID").like("%INSURANCE%")))
            
            unit_insured_max_sev_df=unit_insured_max_sev_pre_df.withColumn("DMG_1_Value",when(regexp_extract(col("VEH_DMAG_SCL_1_ID"),r'(\d+)', 1) == "",0).otherwise(regexp_extract(col("VEH_DMAG_SCL_1_ID"), r'(\d+)', 1).cast("int"))).withColumn("DMG_2_Value",when(regexp_extract(col("VEH_DMAG_SCL_2_ID"),r'(\d+)',1) == "",0).otherwise(regexp_extract(col("VEH_DMAG_SCL_2_ID"),r'(\d+)',1).cast("int"))).filter((col("DMG_1_Value")>4) | (col("DMG_2_Value")>4)).select(col("CRASH_ID")).distinct()
            
            none_cnt=unit_insured_max_sev_df.join(broadcast(none_damage_df),on='CRASH_ID',how='inner').select(col("CRASH_ID")).count()
            
            self.log.log_success(f"Successfully extracted the ninth analysis. "
                                 f"The Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance is: {none_cnt}")

            return {"status": "success", "Count": none_cnt, "Analysis": "No_Damage_With_High_Severity_cnt"}
        
        except Exception as err:
            self.log.log_error(f"Error during none_damage_rating_4_distinct_crash analysis: {err}")
            return {"status": "error", "message": str(err)}
            

    def Top_5_Vehicle_Makes_State_and_Color(self):

        """ Method to Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences.
        From the person_df we are filtering the crashes with valid licences. Then from the units_df we are grouping the vehicle color column and getting the colors with the top 10 count of the crashses and similarly we are grouping the license state column and getting the the state with the top 25 count and converting both the df's into lists.
        After that we are filtering the units_df based on the 3 contributing factors columns to contain the string SPEED and adding the and condition where the state and color column records are present in the respective aggregated lists.
        Once we get this we are joining with person_df to get the top 5 veh make id by grouping them and Once we get the makes we are calling the Data Read Class's df_to_file method and writing the df to a csv file and storing it into the config mentioned output path with the preferred naming.
        """
        try:
            if self.person_df is None:
                person_obj = DataPlay(self.spark)
                self.person_df = person_obj.file_to_df("person_path")
            
            if self.units_df is None:
                unit_obj = DataPlay(self.spark)
                self.units_df = unit_obj.file_to_df("units_path")
                
            person_df_join=self.person_df.filter(col("DRVR_LIC_TYPE_ID").isin(["COMMERCIAL DRIVER LIC.","OCCUPATIONAL","DRIVER LICENSE"])).distinct()    

            top_color_df=self.units_df.filter(~col("VEH_COLOR_ID").isin(["NA","99","98"])).groupBy(col("VEH_COLOR_ID")).count().orderBy(col("count").desc()).limit(10).drop(col("count"))
            top_state_df=self.units_df.filter(~col("VEH_LIC_STATE_ID").isin(["NA","98"])).groupBy(col("VEH_LIC_STATE_ID")).count().orderBy(col("count").desc()).limit(25).drop(col("count"))
            
            state_list = top_state_df.select(col("VEH_LIC_STATE_ID")).collect()
            state_list = [row["VEH_LIC_STATE_ID"] for row in state_list]
            color_list = top_color_df.select(col("VEH_COLOR_ID")).collect()
            color_list = [row["VEH_COLOR_ID"] for row in color_list]
            
            units_df_join=self.units_df.filter((col("CONTRIB_FACTR_1_ID").like("%SPEED%")) | (col("CONTRIB_FACTR_2_ID").like("%SPEED%")) | (col("CONTRIB_FACTR_P1_ID").like("%SPEED%"))).filter((col("VEH_LIC_STATE_ID").isin(state_list)) & col("VEH_COLOR_ID").isin(color_list))
            
            vehicle_make_df=person_df_join.join(broadcast(units_df_join),on=["CRASH_ID","UNIT_NBR"],how="inner").groupBy(col("VEH_MAKE_ID")).count().orderBy(col("count").desc()).limit(5).drop(col("count"))
            
            self.file_write_obj.df_to_file(vehicle_make_df, "Top_Vehicle_Makes_10Color_25State.csv")

            self.log.log_success(f"Successfully extracted the tenth analysis and stored in the file.")
            return {"status": "success", "file_name": "Top_Vehicle_Makes_10Color_25State","Analysis":"Top 5 Vehicle Makes belonging to Top 10 Color and Top 25 States of the Crash and crash due to Speeding"}

        except Exception as err:
            self.log.log_error(f"Error during Top_5_Vehicle_Makes analysis: {err}")
            return {"status": "error", "message": str(err)}
            
