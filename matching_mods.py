import pandas as pd
from itertools import combinations
import sys
from more_itertools import chunked
import os
from reference import MODS, COLUMNS, start_logging, price_df_norm
import datetime
import logging

start_logging('./info_log/matching_mods.log', __name__)
logger = logging.getLogger(__name__)
logger.info('Script started at %s', datetime.datetime.now())

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
    filepath = "./sets/"+ Output[0] +"_sets.csv"
    
    if os.path.exists(filepath):
        os.remove(filepath)
        logger.info("Outdated file removed")
    logger.info("Creating new file") 

    combination_data = price_df_norm(combination_data)
    # combination_data["ROF"] = ((combination_data["ROF"] - 1) * 100).abs()
    data = combination_data[(combination_data["DPS"] >= 26) & (combination_data["Price"] <= 1000000000)]

    combination_data_list = list(data["ID"])
    combination_data_list = combination_data_list
    logger.info("combination_data list created")
    combination_data_list2 = list(combinations(combination_data_list, (4)))
        
    size = 3000000
    chunked_list = list(chunked(combination_data_list2, size))
    logger.info(f"chunked_list size: {size}")

    data = data.astype({
        "Damage": float,
        "ROF": float,
        "CPU": float,
        "Contract": str
        })

    data = data[["ID", "CPU", 'Damage', 'ROF', "DPS", "Contract"]]

    chunkcount = 0
    ship_dict = {"paladin":
        {"damage" :{
            "guns": 3.6
            ,"ammo": 70.8
            ,"hull": 2
            ,"weapon": 1.25
            ,"surgical_strike": 1.15
            ,"spec": 1.1
            ,"mar" : 1.25
            ,"LE-1006": 1.06
            }
        ,"rof": {
            "guns": 7.88
            ,"gunnery": 0.9
            ,"rapid_fire": 0.8
            ,"RF-906": 0.94
            ,"rig": 0.85
            }
        }
    }
for ship in ship_dict:
    dps = 1.0
    rof = 1.0
    for key, value in ship_dict[ship]["damage"].items():
        dps = round(dps * ship_dict[ship]["damage"][key], 3)
    for key, value in ship_dict[ship]["rof"].items():
        rof = round(rof * ship_dict[ship]["rof"][key], 3)

    
    for chunk in chunked_list:
        df = pd.DataFrame(chunk)

        data2 = df.merge(data.add_suffix('_first'), left_on=0,right_on="ID_first",how="left")
        data2 = data2.merge(data.add_suffix('_second'), left_on=1,right_on="ID_second",how="left")
        data2 = data2.merge(data.add_suffix('_third'), left_on=2,right_on="ID_third",how="left")
        data2 = data2.merge(data.add_suffix('_fourth'), left_on=3,right_on="ID_fourth",how="left")

        data2damage = data2[["Damage_first","Damage_second","Damage_third","Damage_fourth"]]
        data2damage = data2damage.rank(method="first", axis=1, ascending=False)
        data2damage = data2damage.replace({2:0.869, 3:0.571, 4:0.283})

        data2rof = data2[["ROF_first","ROF_second","ROF_third","ROF_fourth"]]
        data2rof = data2rof.rank(method="first", axis=1, ascending=False)
        data2rof = data2rof.replace({1:0.869, 2:0.571, 3:0.283, 4:0.106})

        data3 = data2.merge(data2damage.add_suffix('_damagaPenalty'),how="left",left_index=True, right_index=True)
        data3 = data3.merge(data2rof.add_suffix('_rofPenalty'),how="left",left_index=True, right_index=True)
        data3["TOTAL DAMAGE"] = dps
        for col in ["Damage_first", "Damage_second", "Damage_third", "Damage_fourth"]:
            data3[col] = (((data3[col] - 1) * data3[f"{col}_damagaPenalty"]) + 1).round(3)
            data3["TOTAL DAMAGE"] = (data3["TOTAL DAMAGE"] * data3[col]).round(3)

        data3["TOTAL ROF"] = rof
        for col in ["ROF_first","ROF_second","ROF_third","ROF_fourth"]:
            data3[col] = 1 - ((1 - data3[col]) * data3[f"{col}_rofPenalty"])
            data3["TOTAL ROF"] = (data3["TOTAL ROF"] * data3[col]).round(3)

        data3["TOTAL DAMAGE"] = (((data3["TOTAL DAMAGE"] * 4)/ data3["TOTAL ROF"]) * 2).round(3)
        
        
        # data3["TotalDamage"] = (((data3["Damage_first"] - 1) * data3["Damage_first_damagaPenalty"]) + 1) + \
        #     (((data3["Damage_second"] - 1) * data3["Damage_second_damagaPenalty"]) + 1) + \
        #     (((data3["Damage_third"] - 1) * data3["Damage_third_damagaPenalty"]) + 1) + \
        #     (((data3["Damage_fourth"] - 1) * data3["Damage_fourth_damagaPenalty"]) + 1)
        
        # data3["TotalROF"] = (data3["ROF_first"] * data3["ROF_first_rofPenalty"]) + \
        #     (data3["ROF_second"] * data3["ROF_second_rofPenalty"])+ \
        #     (data3["ROF_third"] * data3["ROF_third_rofPenalty"])+ \
        #     (data3["ROF_fourth"] * data3["ROF_fourth_rofPenalty"])
        
        data3["TotalCPU"] = data3["CPU_first"] + data3["CPU_second"] + data3["CPU_third"] + data3["CPU_fourth"] 
        
        if Output[1] == "Market HeatSinks":       
            # Paladin
            # report2 = data3[(data3["TotalDamage"] >= 4.355) & (data3["TotalROF"] >= 33.11)].copy()
            report2 = data3[(data3["TOTAL DAMAGE"] >= 3350)].copy()
        elif Output[1] == "Market GyroStabs": 
            # Vargur 
            report2 = data3[(data3["TotalDamage"] >= 4.369) & (data3["TotalROF"] >= 33.1114)].copy()
        elif Output[1] == "Market MagStabs": 
            # Kronos
            report2 = data3[(data3["TotalDamage"] >= 4.368) & (data3["TotalROF"] >= 31.23)].copy()

        # report3 = report2[[0,1,2,3,"TOTAL DAMAGE"]]     
  
        report2.to_csv(filepath, mode='a', index=False, header=True)
        chunkcount = chunkcount+1
        logger.info(f"{Output[0]} chunk " + str(chunkcount) + " saved.")    
        
    logger.info(f"All {Output[0]} chunks saved.")
    
logger.info('Script completed at %s', datetime.datetime.now())