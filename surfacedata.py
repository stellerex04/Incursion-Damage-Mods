import pandas as pd
import gspread
import sys
from reference import MODS, COLUMNS

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
    data = pd.read_csv(f".\webcrawler\{Output[0]}Output.csv") 
    data = data.drop_duplicates(keep="first")
    
    if "Flag1" in data.columns:
        data = data[data["Flag1"] != "A"]
    if "Flag2" in data.columns:
        data = data[data["Flag2"] != "A"]
    print("webcrawl data cleaned")

    data["Price"] = data["Price"].astype("float64")
    data["Unit"] = data["Unit"].fillna("plex")
    data["Contract"] = data["Contract"].astype(str)

    def price_norm(row):
        if "billion" in row["Unit"]:
            return row["Price"] * 1000000000
        elif "million" in row["Unit"]:
            return row["Price"] * 1000000 
        elif "plex" in row["Unit"]:
            return row["Price"] * 5100000
        else:
            return row["Price"]

    data['Price'] = data.apply(lambda row: price_norm(row), axis=1)
    data["Price"] = data["Price"].astype("int64")

    contract_data = data[["Contract", "Price"]].drop_duplicates(subset=["Contract"],keep="first")
    
    report3 = pd.read_csv("./sets/"+ Output[0] +"_sets.csv",names = COLUMNS)
    
    report3['Contract'] = report3[["Contract_first", "Contract_second", "Contract_third", "Contract_fourth"]].values.tolist()
    report3 = report3.drop(columns=["Contract_first", "Contract_second", "Contract_third", "Contract_fourth"])

    contract_agg = report3['Contract'].explode()
    contract_agg = contract_agg.reset_index().drop_duplicates(subset=['index','Contract']).astype({"Contract": str})
    contract_agg = pd.merge(contract_agg, contract_data, on="Contract", how='left')
    contract_agg["index"] = contract_agg["index"].astype(int)
    contract_agg = contract_agg.groupby("index").agg({"Price":"sum"})
    contract_agg = contract_agg.rename(columns={"Price": "TotalPrice"})
    report3["Contract"] = report3["Contract"].astype(str)
    report3 = pd.concat([report3, contract_agg], axis=1)
    report3 = report3.sort_values(by=["TotalPrice", 'TotalDamage', "TotalROF"], ascending=True).iloc[:2000]


    gc = gspread.oauth()
    sh = gc.open(Output[1])

    outputsheet = sh.worksheet("output")

    if len(report3) < 2000:
        outputsheet.clear()

    outputsheet.update([report3.columns.values.tolist()] + report3.values.tolist())

    with open('./log/' + str(Output[0]) + '.txt', 'r') as f:
        last_line = f.readlines()[-1]
    last_line = last_line.replace("\n", "")

    details = sh.worksheet("details")
    details.update('B1', last_line)

    print(str(Output[1])+" Updated")