import pandas as pd
from itertools import combinations
import sys
from more_itertools import chunked
import os
import datetime
import logging
import math
from reference import MODS, COLUMNS, SHIPS, start_logging, price_df_norm
from pathlib import Path

start_logging('./info_log/matching_mods.log', __name__)
logger = logging.getLogger(__name__)

chunkcount = 0

def stacking_penalty(u):
    result = math.exp(-(u/2.67)**2)
    return result

stacking_penalty_1 = stacking_penalty(1)
stacking_penalty_2 = stacking_penalty(2)
stacking_penalty_3 = stacking_penalty(3)
stacking_penalty_4 = stacking_penalty(4)

def matching_mods(mod_dict, set_size: int, min_percent: int = 27, max_price: int = 1000000000):

    logger.info('Script started at %s', datetime.datetime.now())
    combination_data = pd.read_csv(f".\webcrawler\{mod_dict[0]}Output.csv") 
    filepath = f"./sets/{mod_dict[0]}_{set_size}.parquet"

    if os.path.exists(filepath):
        os.remove(filepath)
        logger.info("Outdated file removed")
    logger.info(f"Creating new file: {filepath}") 

    combination_data = price_df_norm(combination_data)
    data = combination_data[(combination_data["DPS"] >= min_percent) & (combination_data["Price"] <= max_price)]

    combination_data_list = list(data["ID"])
    logger.info("combination_data list created")
    combination_data_list2 = list(combinations(combination_data_list, (set_size)))
        
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

    data = data[["ID", "CPU", 'Damage', 'ROF', "Contract"]]

    ship_stats = SHIPS[mod_dict[0]]
    dps = 1.0
    rof = 1.0
    for key, value in ship_stats["damage"].items():
        dps = dps * ship_stats["damage"][key]

    for key, value in ship_stats["rof"].items():
        rof = rof * ship_stats["rof"][key]

    chunkcount = 0
    for chunk in chunked_list:
        df = pd.DataFrame(chunk)
        mod_cols = df.columns
        damage_cols = []
        rof_cols = []
        
        for col in mod_cols:
            df = df.merge(data.add_suffix(f"_{col}"), left_on=col, right_on=f"ID_{col}",how="left")
            damage_cols.append(f"Damage_{col}")
            rof_cols.append(f"ROF_{col}")
        
        data2damage = df[damage_cols]
        data2damage = data2damage.rank(method="first", axis=1, ascending=False)
        data2damage = data2damage.replace({2:stacking_penalty_1, 3:stacking_penalty_2, 4:stacking_penalty_3})

        data2rof = df[rof_cols]
        data2rof = data2rof.rank(method="first", axis=1, ascending=True)
        data2rof = data2rof.replace({1:stacking_penalty_1, 2:stacking_penalty_2, 3:stacking_penalty_3, 4:stacking_penalty_4})
        
        df = df.merge(data2damage.add_suffix('_damagaPenalty'), how="left", left_index=True, right_index=True)
        df = df.merge(data2rof.add_suffix('_rofPenalty'), how="left", left_index=True, right_index=True)
        
        df["raw_damage"] = dps
        for col in damage_cols:
            df[col] = ((df[col] - 1) * df[f"{col}_damagaPenalty"]) + 1 
            df["raw_damage"] = df["raw_damage"] * df[col]

        df["raw_rof"] = rof
        for col in rof_cols:
            df[col] = 1 - ((1 - df[col]) * df[f"{col}_rofPenalty"])
            df["raw_rof"] = df["raw_rof"] * df[col]

        df["Total Damage"] = ((df["raw_damage"] * 4) / (df["raw_rof"]/1000)) * 2
        
        df["Total CPU"] = 0
        for col in mod_cols:
            df["Total CPU"] = df["Total CPU"] + df[f"CPU_{col}"]

        if mod_dict[0] == "HeatSink":       
            df = df[(df["Total Damage"] >= 3350)]
        elif mod_dict[0] == "GyroStab": 
            df = df[(df["Total Damage"] >= 3350)]
        elif (mod_dict[0] == "MagStab") & (set_size == 4):
            df = df[(df["Total Damage"] >= 4100)]
        elif (mod_dict[0] == "MagStab") & (set_size == 3): 
            df = df[(df["Total Damage"] >= 3900)]
        else:
            raise ValueError()

        for col in mod_cols:
            df.rename(columns={col: f"MOD{col + 1}"}, inplace=True)
        
        df = df.filter(regex='Total Damage|Total CPU|MOD*|Contract_*')

        if os.path.exists(filepath):
            df.to_parquet(filepath, engine='fastparquet', append=True, index=False)
        else:
            df.to_parquet(filepath, engine='fastparquet', index=False)
            
        chunkcount = chunkcount+1
        logger.info(f"{mod_dict[0]} chunk " + str(chunkcount) + " saved.")    
        
    logger.info(f"All {mod_dict[0]} chunks saved.")

    logger.info('Script completed at %s', datetime.datetime.now())