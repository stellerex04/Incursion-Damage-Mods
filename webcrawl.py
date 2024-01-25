import sys
import datetime
from reference import MODS, start_logging
import pandas as pd
import requests 
import json
import logging


start_logging('./info_log/webcrawl.log', __name__)
logger = logging.getLogger(__name__)
logger.info('Script started at %s', datetime.datetime.now())

mods = MODS
if len(sys.argv) > 1:
    if sys.argv[1] == "HeatSink":
        mods = [MODS[0]]
    elif sys.argv[1] == "MagStab":
        mods = [MODS[1]]
    elif sys.argv[1] == "GyroStab":
        mods = [MODS[2]]
else:
    mods = MODS

for mod in mods:
    attempts = 0
    success = False
    while attempts < 3 and not success:
        try:
            mod_url = f"https://mutaplasmid.space/api/old/type/49726/available/?_={mod[3]}"
            response = requests.get(mod_url)
            df = pd.DataFrame(json.loads(response.text))
            df2=df
            df2["ID"] = f"https://mutaplasmid.space/module/" + df2['id'].astype(str) + "/"
            df2 = df2[df2["static"] == False]
            attributes = pd.json_normalize(df2["attributes"])[[
                '204.real_value',
                '64.real_value', 
                '50.real_value', 
                '100004.real_value'
                ]].rename(columns={
                '204.real_value': "ROF", 
                '64.real_value': "Damage",
                '50.real_value': "CPU",
                '100004.real_value': "DPS"
                })
            
            df2 = pd.concat([df2.drop(columns=["attributes"]),attributes], axis=1)
            price = pd.json_normalize(df2["contract"])[["id", "price.plex", "price.isk","auction","multi_item"]].rename(columns={"id":"Contract"}).astype({"price.plex":"float64", "price.isk": "float64"})
            price = price[price["auction"] == False]
            price["Price"] = (price["price.isk"] + (price["price.plex"]* 5100000))
            df2 = pd.concat([df2.drop(columns=["contract"]),price.drop(columns=["price.plex", "price.isk"])], axis=1)
            df2 = df2.drop(columns=["type_id","price_prediction","price_prediction_date","module_name","static"])
            df2 = df2[df2["Contract"].notna()]
            df2["Contract"] = df2["Contract"].astype("int").astype("str")
            df2["Price"] = df2["Price"].astype("int64")
            df2.to_csv("./webcrawler/" + str(mod[0]) + "_api_output.csv",index=False)
            logger.info(f"{mod[0]} Saved")
            
            t = datetime.datetime.now()
            timestamp = t.strftime("[%d.%m.%y] Time - %H_%M_%S")
            log_msg = str(timestamp)
            logger.info(log_msg)
            with open('./log/' + str(mod[0]) + '.txt','a') as file:
                file.write(log_msg + "\n")

            logger.info(f"{mod[0]} Log Updated")
            success = True

        except Exception as e:
            logger.info(e)
            attempts += 1
            logger.info(f"Attempt:{attempts}")
            if attempts >= 3:
                logger.info(f"{mod[0]} Failed")
                raise ValueError("Failed run.")

logger.info('Script completed at %s', datetime.datetime.now())