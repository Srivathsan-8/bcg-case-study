from source.BCG_Analysis_Methods import BCGAnalysis
from source.BCG_Data_Load_and_Write import DataPlay
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("BCG_Case_Study").getOrCreate()
    analysis = BCGAnalysis(spark)
    output_loc_obj = DataPlay(spark)

    config_df = output_loc_obj.get_app_conf_dict()
    op = config_df["output_path"]

    count_analysis = {}
    file_analysis = {}

    def append_results(res, count_analysis, file_analysis):
        if res["status"] == "success":
            if "file_name" in res:
                f_string = f"The Analysis File Path :{op}/"
                file_analysis[res["Analysis"]] = f_string + res["file_name"]
            if "Count" in res:
                count_analysis[res["Analysis"]] = res["Count"]

    # List of methods to call for count analysis
    count_analysis_methods = [
        analysis.male_accidents,
        analysis.two_wheeler_accidents,
        analysis.hit_and_run_valid_df,
        analysis.none_damage_rating_4_distinct_crash
    ]

    # List of methods to call for file analysis
    file_analysis_methods = [
        analysis.vehicle_airbagnotdeployed_killed,
        analysis.female_not_involved_state,
        analysis.third_to_fifth_injury_model,
        analysis.ethnic_user_make_id,
        analysis.alc_driver_zip_code,
        analysis.Top_5_Vehicle_Makes_State_and_Color
    ]

    # Execute count analysis methods
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
