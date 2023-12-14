import pandas as pd
from itertools import combinations
import sys
from more_itertools import chunked
import os
import datetime
import logging
import math
from reference import MODS, COLUMNS, SHIPS, start_logging, price_df_norm

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


def stacking_penalty(u):
    result = math.exp(-(u/2.67)**2)
    return result

stacking_penalty_1 = stacking_penalty(1)
stacking_penalty_2 = stacking_penalty(2)
stacking_penalty_3 = stacking_penalty(3)
stacking_penalty_4 = stacking_penalty(4)

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
    logger.info(f"chunked_list len: {len(chunked_list)}")
    data = data.astype({
        "Damage": float,
        "ROF": float,
        "CPU": float,
        "Contract": str
        })

    data = data[["ID", "CPU", 'Damage', 'ROF', "DPS", "Contract"]]

    ship_stats = SHIPS[Output[0]]
    for ship in ship_stats:
        dps = 1.0
        rof = 1.0
        for key, value in ship_stats["damage"].items():
            dps = dps * ship_stats["damage"][key]

        for key, value in ship_stats["rof"].items():
            rof = rof * ship_stats["rof"][key]

    chunkcount = 0
    for chunk in chunked_list:
        df = pd.DataFrame(chunk)

        data2 = df.merge(data.add_suffix('_first'), left_on=0,right_on="ID_first",how="left")
        data2 = data2.merge(data.add_suffix('_second'), left_on=1,right_on="ID_second",how="left")
        data2 = data2.merge(data.add_suffix('_third'), left_on=2,right_on="ID_third",how="left")
        data2 = data2.merge(data.add_suffix('_fourth'), left_on=3,right_on="ID_fourth",how="left")

        data2damage = data2[["Damage_first","Damage_second","Damage_third","Damage_fourth"]]
        data2damage = data2damage.rank(method="first", axis=1, ascending=False)
        data2damage = data2damage.replace({2:stacking_penalty_1, 3:stacking_penalty_2, 4:stacking_penalty_3})

        data2rof = data2[["ROF_first","ROF_second","ROF_third","ROF_fourth"]]
        data2rof = data2rof.rank(method="first", axis=1, ascending=True)
        data2rof = data2rof.replace({1:stacking_penalty_1, 2:stacking_penalty_2, 3:stacking_penalty_3, 4:stacking_penalty_4})

        data3 = data2.merge(data2damage.add_suffix('_damagaPenalty'),how="left",left_index=True, right_index=True)
        data3 = data3.merge(data2rof.add_suffix('_rofPenalty'),how="left",left_index=True, right_index=True)
        data3["raw_damage"] = dps
        for col in ["Damage_first", "Damage_second", "Damage_third", "Damage_fourth"]:
            data3[col] = ((data3[col] - 1) * data3[f"{col}_damagaPenalty"]) + 1 
            data3["raw_damage"] = data3["raw_damage"] * data3[col]

        data3["raw_rof"] = rof
        for col in ["ROF_first","ROF_second","ROF_third","ROF_fourth"]:
            data3[col] = 1 - ((1 - data3[col]) * data3[f"{col}_rofPenalty"])
            data3["raw_rof"] = data3["raw_rof"] * data3[col]

        data3["Total Damage"] = ((data3["raw_damage"] * 4) / (data3["raw_rof"]/1000)) * 2
        data3["TotalCPU"] = data3["CPU_first"] + data3["CPU_second"] + data3["CPU_third"] + data3["CPU_fourth"] 
        
        if Output[1] == "Market HeatSinks":       
            # Paladin
            data3 = data3[(data3["Total Damage"] >= 3350)].copy()
        elif Output[1] == "Market GyroStabs": 
            # Vargur 
            data3 = data3[(data3["Total Damage"] >= 3350)].copy()
        elif Output[1] == "Market MagStabs": 
            # Kronos
            data3 = data3[(data3["Total Damage"] >= 4100)].copy()

        data3 = data3[COLUMNS]    
        data3.to_csv(filepath, mode='a', index=False, header=False)
        chunkcount = chunkcount+1
        logger.info(f"{Output[0]} chunk " + str(chunkcount) + " saved.")    
        
    logger.info(f"All {Output[0]} chunks saved.")
    
logger.info('Script completed at %s', datetime.datetime.now())