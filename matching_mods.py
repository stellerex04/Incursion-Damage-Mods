import pandas as pd
from itertools import combinations
import sys
from more_itertools import chunked
import os
from reference import MODS


if len(sys.argv) > 1:
    if sys.argv[1] == "HeatSink":
        Files = [MODS[0]]
    elif sys.argv[1] == "MagStab":
        Files = [MODS[1]]
    elif sys.argv[1] == "GyroStab":
        Files = [MODS[2]]
else:
    Files = MODS

chunkcount = 0

for Output in Files:
    combination_data = pd.read_csv(f".\webcrawler\{Output[0]}Output.csv") 
    combination_data = combination_data.drop_duplicates(keep="first")
    
    if "Flag1" in combination_data.columns:
        combination_data = combination_data[combination_data["Flag1"] != "A"]
    if "Flag2" in combination_data.columns:
        combination_data = combination_data[combination_data["Flag2"] != "A"]
    print("webcrawl data cleaned")

    filepath = "./sets/"+ Output[0] +"_sets.csv"
    
    if os.path.exists(filepath):
        os.remove(filepath)
        print("Old file removed")
    else:
        print("The file does not exist") 

    data = combination_data[(combination_data["DPS"] >= 26)]
    combination_data_list = list(data["ID"])
    combination_data_list = combination_data_list
    print("combination_data list created")
    combination_data_list2 = list(combinations(combination_data_list, (4)))
        
    size = 3000000
    chunked_list = list(chunked(combination_data_list2, size))
    print(f"chunked_list size: {size}")

    data = data.astype({
        "Damage": float,
        "ROF": float,
        "CPU": float,
        "Contract": str
        })

    data = data[["ID", "CPU", 'Damage', 'ROF', "DPS", "Contract"]]

    chunkcount = 0
    
    for chunk in chunked_list:
        df = pd.DataFrame(chunk)

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
        data3["TotalCPU"] = data3["CPU_first"] + data3["CPU_second"] + data3["CPU_third"] + data3["CPU_fourth"] 
        
        if Output[1] == "Market HeatSinks":       
            # Paladin
            report2 = data3[(data3["TotalDamage"] >= 3.0784) & (data3["TotalROF"] >= 33.11)].copy()
        elif Output[1] == "Market GyroStabs": 
            # Vargur 
            report2 = data3[(data3["TotalDamage"] >= 3.093194) & (data3["TotalROF"] >= 33.1114)].copy()
        elif Output[1] == "Market MagStabs": 
            # Kronos
            report2 = data3[(data3["TotalDamage"] >= 3.093192) & (data3["TotalROF"] >= 31.23)].copy()
        
        report3 = report2[[0,1,2,3,"TotalDamage","TotalROF", "TotalCPU", "Contract_first", "Contract_second", "Contract_third", "Contract_fourth"]]       
  
        report3.to_csv(filepath, mode='a', index=False, header=False)
        chunkcount = chunkcount+1
        print(f"{Output[0]} chunk" + str(chunkcount) + " saved.")    
        
    print(f"All {Output[0]} chunks saved.")
    