from source.BCG_Analysis_Methods import BCGAnalysis
from source.BCG_Data_Load_and_Write import DataPlay
from pyspark.sql import SparkSession

def main():
    
    """
    Initialising the objects with the necessary modules
    """
    spark = SparkSession.builder.appName("BCG_Case_Study").getOrCreate()
    analysis = BCGAnalysis(spark)
    output_loc_obj = DataPlay(spark)
    """
    Calling the Config Method from the DataPlay Class
    to get the output path for concatenating with the the 
    file location.
    """
    config_df = output_loc_obj.get_app_conf_dict()
    op = config_df["output_path"]

    """
    Initialising two dictionaries each for Count Analysis and File Analysis.
    """
    count_analysis = {}
    file_analysis = {}

    """
    Function for adding the records and data to the dictionaries by checking
    if the status of the code is Success.
    If it's a file based analysis We'll get file path from the Analysis Class along with
    the name or if it's a count based analysis we'll get the count for the same.
    """
    def append_results(res, count_analysis, file_analysis):
        if res["status"] == "success":
            if "file_name" in res:
                f_string = f"The Analysis File Path :{op}/"
                file_analysis[res["Analysis"]] = f_string + res["file_name"]
            if "Count" in res:
                count_analysis[res["Analysis"]] = res["Count"]

    """
    Segregating the 10 Analysis into two different types of analysis based on count
    and file based and appending it to the two different list.
    """
    count_analysis_methods = [
        analysis.male_accidents,
        analysis.two_wheeler_accidents,
        analysis.hit_and_run_valid_df,
        analysis.none_damage_rating_4_distinct_crash
    ]

    file_analysis_methods = [
        analysis.vehicle_airbagnotdeployed_killed,
        analysis.female_not_involved_state,
        analysis.third_to_fifth_injury_model,
        analysis.ethnic_user_make_id,
        analysis.alc_driver_zip_code,
        analysis.Top_5_Vehicle_Makes_State_and_Color
    ]


    """
    Iterating through both the list and calling the class and passing the output of
    the class to the append_results function to get the count and file paths
    """
    for method in count_analysis_methods:
        result = method()
        append_results(result, count_analysis, file_analysis)

    # Execute file analysis methods
    for method in file_analysis_methods:
        result = method()
        append_results(result, count_analysis, file_analysis)

    print(count_analysis)
    print(file_analysis)

if __name__ == "__main__":
    main()
