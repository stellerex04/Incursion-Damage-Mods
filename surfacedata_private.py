import numpy as np
import pandas as pd
from itertools import combinations
import gspread
import sys
from more_itertools import chunked
import os


Directories = [['D:\EVE Data\set collection\python\webcrawler\HeatSinkWebCrawlOutput.csv', 'Private HeatSinks','HeatSink'],
['D:\EVE Data\set collection\python\webcrawler\MagStabWebCrawlOutput.csv','Market MagStabs','MagStab']] 

#three possible mag, heat, and private mag or heat input file?
#make public work first
# Files = [Directories[1]]

if len(sys.argv) > 1:
    if sys.argv[1] == "HeatSink":
        Files = [Directories[0]]
    elif sys.argv[1] == "MagStab":
        Files = [Directories[1]]
else:
    Files = Directories

ChuckCount = 0

for Output in Files:
    combination_data = pd.read_csv(Output[0]) 
    combination_data = combination_data.drop_duplicates(keep="first")
    print("combination_data cleaned")

    filepath = "D:\EVE Data\set collection\python\sets\\"+ Output[2] +"\sets.csv"
    
    if os.path.exists(filepath):
        os.remove(filepath)
        print("Old file removed")
    else:
        print("The file does not exist") 

    # if len(combination_data) > 150:
    #     combination_data[combination_data["DPS"] >= 26]
    #     combination_data[combination_data["DPS"] <= 29]

    gc = gspread.oauth()
    sh = gc.open(Output[1])

 
    combination_data = combination_data[(combination_data["DPS"] >= 26)]

    inputsheet = sh.worksheet("input")
    inputdf = pd.DataFrame(inputsheet.get_all_records())
    required_count = 0

    if len(inputdf) > 0:
        inputdf_list = list(inputdf["ID"])

        inputdf[["CPU","trash"]] = inputdf["CPU"].str.split(' ',expand=True) 
        inputdf[["Damage","trash"]] = inputdf["Damage"].str.split(' ',expand=True) 
        inputdf[["ROF","trash"]] = inputdf["ROF"].str.split(' ',expand=True) 
        inputdf = inputdf.drop(columns=["trash"])
        
        required_mods = inputdf[inputdf["REQUIRED IN SET"] == "TRUE"]
        required_mods = required_mods.drop(columns=['REQUIRED IN SET'])

        user_mods = inputdf[inputdf["REQUIRED IN SET"] == "FALSE"]
        user_mods = user_mods.drop(columns=['REQUIRED IN SET'])
        
        combination_data = pd.concat([combination_data, user_mods])

        required_count = len(required_mods)
        required_mods_id = list(required_mods["ID"])
        user_mods_id = list(user_mods["ID"])
        print("Required Mods Found")
    else:
        print("No Required Mods Found")

    combination_data_list = list(combination_data["ID"])
    combination_data_list = combination_data_list
    print("combination_data list created")
    combination_data_list2 = list(combinations(combination_data_list, (4-required_count)))
    # combination_data_list2 = list(combinations(combination_data_list, (4)))
      
    size = 1000000
    chunked_list = list(chunked(combination_data_list2, size))



    data = pd.concat([combination_data, required_mods])

    data["Damage"] = data["Damage"].astype(float)
    data["ROF"] = data["ROF"].astype(float)
    data["Price"] = data["Price"].astype(float)
    data["Unit"] = data["Unit"].fillna("plex")

    def clean_price(row):
        if "billion" in row["Unit"]:
            return row["Price"] * 1000000000
        elif "million" in row["Unit"]:
            return row["Price"] * 1000000 
        elif "plex" in row["Unit"]:
            return row["Price"] * 5100000
        else:
            return row["Price"]
        
    data['RawPrice'] = data.apply(clean_price, axis=1)

    data['RawPrice'] = data['RawPrice'].apply(np.int64)

    data = data[["ID",'Damage', 'ROF', "DPS", 'RawPrice']]
    
    report_output  = []
    ChuckCount = 0
    
    for chunk in chunked_list:
        df = pd.DataFrame(chunk)

        if len(required_mods_id) > 0:
            for item in required_mods_id:
                df[len(df.columns)] = item

        data2 = df.merge(data.add_suffix('_first'), left_on=0,right_on="ID_first",how="left")
        data2 = data2.merge(data.add_suffix('_second'), left_on=1,right_on="ID_second",how="left")
        data2 = data2.merge(data.add_suffix('_third'), left_on=2,right_on="ID_third",how="left")
        data2 = data2.merge(data.add_suffix('_fourth'), left_on=3,right_on="ID_fourth",how="left")

        data2damage = data2[["Damage_first","Damage_second","Damage_third","Damage_fourth"]]
        data2damage = data2damage.rank(method= "first",axis=1,ascending=False)
        data2damage = data2damage.replace(2,0.869).replace(3,0.571).replace(4,0.283)

        data2rof = data2[["ROF_first","ROF_second","ROF_third","ROF_fourth"]]
        data2rof = data2rof.rank(method= "first",axis=1,ascending=False)
        data2rof = data2rof.replace(2,0.869).replace(3,0.571).replace(4,0.283)

        data3 = data2.merge(data2damage.add_suffix('_damagaPenalty'),how="left",left_index=True, right_index=True)
        data3 = data3.merge(data2rof.add_suffix('_rofPenalty'),how="left",left_index=True, right_index=True)


        data3["TotalDamage"] = (data3["Damage_first"] * data3["Damage_first_damagaPenalty"]) + (data3["Damage_second"] * data3["Damage_second_damagaPenalty"])+ (data3["Damage_third"] * data3["Damage_third_damagaPenalty"])+ (data3["Damage_fourth"] * data3["Damage_fourth_damagaPenalty"])
        data3["TotalROF"] = (data3["ROF_first"] * data3["ROF_first_rofPenalty"]) + (data3["ROF_second"] * data3["ROF_second_rofPenalty"])+ (data3["ROF_third"] * data3["ROF_third_rofPenalty"])+ (data3["ROF_fourth"] * data3["ROF_fourth_rofPenalty"])
        data3["PRICE"] = data3["RawPrice_first"] + data3["RawPrice_second"] + data3["RawPrice_third"] + data3["RawPrice_fourth"] 

        report = data3[[0,1,2,3,"TotalDamage","TotalROF","PRICE"]].copy()    

        if Output[2] == "HeatSinks":            
            #Paladin Requirements [3.08] Damage [33] 
            report2 = report[(report["TotalDamage"] >= 3.078) & (report["TotalROF"] >= 33)]
        elif Output[2] == "MagStabs": 
            # kronos requirements damage 3.9 and rof 40
            report2 = report[(report["TotalDamage"] >= 3.085) & (report["TotalROF"] >= 31) & (report["PRICE"] <= 3000000000)]
      
  
        report2.to_csv(filepath, mode='a', index=False, header=False)
        ChuckCount = ChuckCount+1
        print("Chuck " + str(ChuckCount) + " Saved")    
        
    print("All Chucks Saved")

    report3 = pd.read_csv(filepath,names = [0, 1, 2, 3, 'TotalDamage', 'TotalROF', 'PRICE'])

    report3 = report3.sort_values(by=["PRICE",'TotalDamage',"TotalROF",], ascending=True).iloc[:2000]
    outputsheet = sh.worksheet("output")

    if len(report3) < 2000:
        outputsheet.clear()

    outputsheet.update([report3.columns.values.tolist()] + report3.values.tolist())

    with open('D:\EVE Data\set collection\python\log\\' + str(Output[2]) + '.txt', 'r') as f:
        last_line = f.readlines()[-1]
    last_line = last_line.replace("\n", "")

    details = sh.worksheet("details")
    details.update('B1', last_line)

    print(str(Output[1])+" Updated")