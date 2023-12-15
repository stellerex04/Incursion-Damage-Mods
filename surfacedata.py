import pandas as pd
import gspread
import sys
from reference import MODS, COLUMNS, start_logging, price_df_norm
import datetime
import logging

start_logging('./info_log/surfacedata.log', __name__)
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
    
for Output in Files:
    logger.info(f"Loading file: {Output[0]}")
    data = pd.read_csv(f".\webcrawler\{Output[0]}Output.csv") 
    data = price_df_norm(data)

    contract_data = data[["Contract", "Price"]].drop_duplicates(subset=["Contract"],keep="first")
    report3 = pd.read_parquet(f"./sets/{Output[0]}_sets.parquet")
    report3['Contract'] = report3[["Contract_first", "Contract_second", "Contract_third", "Contract_fourth"]].values.tolist()
    report3 = report3.drop(columns=["Contract_first", "Contract_second", "Contract_third", "Contract_fourth"])

    contract_agg = report3['Contract'].explode()
    contract_agg = contract_agg.reset_index().drop_duplicates(subset=['index','Contract']).astype({"Contract": str})
    contract_agg = pd.merge(contract_agg, contract_data, on="Contract", how='left')
    contract_agg["index"] = contract_agg["index"].astype(int)
    contract_agg = contract_agg.groupby("index").agg({"Price":"sum"})
    contract_agg = contract_agg.rename(columns={"Price": "Total Price"})
    logger.info(f"Price Total calculated: {Output[0]}")
    report3["Contract"] = report3["Contract"].astype(str)
    report3 = pd.concat([report3, contract_agg], axis=1)
    report3 = report3.sort_values(by=["Total Price", 'Total Damage'], ascending=True).iloc[:3000]
    logger.info(f"Sort completed: {Output[0]}")

    gc = gspread.oauth()
    sh = gc.open(Output[1])

    outputsheet = sh.worksheet("output")
    outputsheet.clear()
    outputsheet.update([report3.columns.values.tolist()] + report3.values.tolist())

    with open('./log/' + str(Output[0]) + '.txt', 'r') as f:
        last_line = f.readlines()[-1]
    last_line = last_line.replace("\n", "")

    details = sh.worksheet("details")
    details.update('B1', last_line)

    logger.info(f"Google Sheet updated: {Output[0]}")
    
logger.info('Script completed at %s', datetime.datetime.now())