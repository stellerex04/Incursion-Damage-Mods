import pandas as pd
import gspread
import sys
from reference import start_logging
import datetime
import logging

start_logging('./info_log/surfacedata.log', __name__)
logger = logging.getLogger(__name__)
logger.info('Script started at %s', datetime.datetime.now())

def surface_data(Output, set_size: int, targets: list(), personal: str = None):

    if personal:
        filename = f"{Output[0]}_{set_size}_{personal}"
    else:
        filename = f"{Output[0]}_{set_size}"

    logger.info(f"Loading file: {Output[0]}")
    data = pd.read_csv(f".\webcrawler\{Output[0]}_api_output.csv") 
    contract_data = data[["Contract", "Price"]].drop_duplicates(subset=["Contract"],keep="first")

    for target in targets:
        report3 = pd.read_parquet(f"./sets/{filename}{target[1]}.parquet")
        report3['Contract'] = report3.filter(regex='Contract_*').values.tolist()
        filtered_columns = report3.columns[~report3.columns.str.contains("Contract_")]
        report3 = report3[filtered_columns]

        contract_agg = report3['Contract'].explode()
        contract_agg = contract_agg.reset_index().drop_duplicates(subset=['index','Contract']).astype({"Contract": str})
        contract_agg = contract_agg.merge(contract_data, on="Contract", how='left')
        contract_agg["index"] = contract_agg["index"].astype(int)
        contract_agg = contract_agg.groupby("index").agg({"Price":"sum"})
        contract_agg = contract_agg.rename(columns={"Price": "Total Price"})
        logger.info(f"Price Total calculated: {Output[0]}")
        report3["Contract"] = report3["Contract"].astype(str)
        report3 = pd.concat([report3, contract_agg], axis=1)
        gc = gspread.oauth()
        
        if personal:
            current_dps = report3[report3["Total Price"] == 0]["Total Damage"].max()
            report3 = report3[report3["Total Damage"] >= current_dps]
            report3 = report3.sort_values(by=[ "Total Price", 'Total Damage'], ascending=True).iloc[:5000]
            logger.info(f"Sort completed: {Output[0]}")
            sh = gc.open_by_key(personal)
            outputsheet = sh.worksheet("output")
            outputsheet.clear()
            outputsheet.update([report3.columns.values.tolist()] + report3.values.tolist())
        else:
        
            output_report = report3[report3["Total Damage"] >= target[0]]
            output_report = output_report.sort_values(by=["Total Price", 'Total Damage'], ascending=True).iloc[:5000]
            logger.info(f"Sort completed: {Output[0]}")
            sh = gc.open(Output[1])
            title = f"{set_size} Set: DPS {target[0]}"
            try:
                outputsheet = sh.worksheet(title)
            except:
                outputsheet = sh.add_worksheet(title=title, rows=5001, cols= 4 + set_size)
                
            outputsheet.clear()
            outputsheet.update([output_report.columns.values.tolist()] + output_report.values.tolist())
            
            if set_size == 3:
                format_col = "G"
            else:
                format_col = "H"

            outputsheet.format(format_col, { "numberFormat": { "type": "NUMBER","pattern": "#,##0" }})

            # TODO  sort and show higher end dps
            # report3 = report3[report3["Total Damage"] >= 3400]
            # low_cost = report3.sort_values(by=["Total Price", 'Total Damage'], ascending=True)
            # logger.info(f"Sort completed: {Output[0]}")
            # outputsheet = sh.worksheet(f"{set_size} Set - Med")
            # outputsheet.clear()
            # outputsheet.update([low_cost.columns.values.tolist()] + low_cost.values.tolist())

    with open('./log/' + str(Output[0]) + '.txt', 'r') as f:
        last_line = f.readlines()[-1]
    last_line = last_line.replace("\n", "")

    details = sh.worksheet("details")
    details.update('B1', last_line)

    logger.info(f"Google Sheet updated: {Output[0]}")
    
    logger.info('Script completed at %s', datetime.datetime.now())