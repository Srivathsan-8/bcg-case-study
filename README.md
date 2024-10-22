Hi All,

This the repo for BCG Case Study on US Accidents. The Source_File_CSV Contains 6 CSV Files from which we have to draw our analysis based on the Problem Statement provided in the mail.

As per the statement I had made sure the input and output is completely config driven from the config.cfg file.

With respect to the Python Scripts, We have developed a main script BCG_Case_Study_Main.py which calls two modules which are python scripts present in the source folder.
I have considered the problem statements as Count Analysis and File Analysis, So whenever the problem statement is to find the count, the application will return a dictionary with the analysis name and the respective count as the value for it.

Similarly when it's a statement for finding the top 5 vehicle makes of such, I will return a csv file containing the values and store it in the Target_Location Folder. As a output even that comes out as a dictionary with analysis name and the file location as the key.

{'Male_Accidents_more_than_2_cnt': 0, 'Two_Wheeler_Accidents_cnt': 781, 'Hit&Run_With_License_cnt': 2616, 'No_Damage_With_High_Severity_cnt': 8}
{'Top 5 Vehicle Makes with Airbags Not Deployed and Person Killed': 'The Analysis File Path :Target_Location/Top_Vehicle_Makes_AB_Not_Deployed', 'Top State where Females where not involved': 'The Analysis File Path :Target_Location/Female_Not_Involved_State', 'Top 3_5 Vehicle Makes Contributing to Highest Injury with Death': 'The Analysis File Path :Target_Location/Top_3_5_Inj_Death_Model', 'Top Ethnicity for Each Vehicle Body': 'The Analysis File Path :Target_Location/Top_Ethnicity_Vehicle_Body', 'Top 5 Zip Codes with Crash Contributing to Alcohol': 'The Analysis File Path :Target_Location/Zip_Code_Drunk_Drive', 'Top 5 Vehicle Makes belonging to Top 10 Color and Top 25 States of the Crash and crash due to Speeding': 'The Analysis File Path :Target_Location/Top_Vehicle_Makes_10Color_25State'}



