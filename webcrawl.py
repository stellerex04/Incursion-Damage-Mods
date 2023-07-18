import pandas as pd
import numpy as np
import sys
import time
import datetime
from pandas.io.html import read_html
from reference import MODS

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
options = Options()
options.add_argument("start-maximized")
options.add_argument("disable-infobars")
options.add_argument("--disable-extensions")
options.add_argument('--disable-blink-features=AutomationControlled')


# LinkData = [['HeatSink', 'https://mutaplasmid.space/type/49726/contracts/'], 
#             ['MagStab', 'https://mutaplasmid.space/type/49722/contracts/'], 
#             ["GyroStab","https://mutaplasmid.space/type/49730/contracts/"]]


if len(sys.argv) > 1:
    if sys.argv[1] == "HeatSink":
        URLs = [MODS[0]]
    elif sys.argv[1] == "MagStab":
        URLs = [MODS[1]]
    elif sys.argv[1] == "GyroStab":
        URLs = [MODS[2]]
else:
    URLs = MODS

for URL in URLs:
    attempts = 0
    success = False
    while attempts < 3 and not success:
        try:
            driver = webdriver.Chrome(options=options)
            print(f"Accesing: https://mutaplasmid.space/type/{URL[2]}/contracts/")
            driver.get(f"https://mutaplasmid.space/type/{URL[2]}/contracts/")
            time.sleep(5)
            Data = []

            while True:
                try:

                    rows = WebDriverWait(driver,50).until(EC.visibility_of_all_elements_located((By.XPATH, '/html/body/div[6]/div/div/div/table/tbody')))
                    for row in rows:
                        Data.append(row.get_attribute('outerHTML'))
                    driver.execute_script("return arguments[0].scrollIntoView(true);", WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,'//*[@class="paginate_button next"]' ))))
                    nextlink = driver.find_element(By.XPATH,'//*[@class="paginate_button next"]')
                    driver.execute_script("arguments[0].click();", nextlink)
                    print("Navigating to Next Page")
                except (TimeoutException, WebDriverException) as e:
                    driver.close()
                    print("Closing Browser Session")
                    break

            Data2 = [element.replace('tbody', 'table') for element in Data]
            Data2 = [element.replace('<button class="btn btn-std-size btn-primary btn-copy btn-copy-contract-link btn-open-contract" data-clipboard-text="<url', '<a href="') for element in Data2]
            Data2 = [element.replace("""</url>">""", '') for element in Data2]
            Data2 = [element.replace('Link</button>', '</a>') for element in Data2]
            Data2 = [element.replace('>Contract', """">Contract""") for element in Data2]
            Data2 = [element.replace("""<button class="btn btn-std-size btn-primary btn-copy" data-clipboard-text=""", "<a href= ") for element in Data2]
            Data2 = [element.replace("""<span class="annotation-auction text-muted" title="This is an auction."></span>""", """<td><span class="annotation-auction text-muted" title="This is an auction.">A</span></td>""") for element in Data2]
            Data2 = [element.replace("""<span class="annotation-multi-item text-muted" title="This contract includes other items."></span>""", """<td><span class="annotation-multi-item text-muted" title="This contract includes other items.">M</span></td>""") for element in Data2]

            htmldf = pd.DataFrame()

            for item in Data2:
                parseddata = read_html(item, extract_links = "all")
                x = np.array(parseddata)
                x = x.reshape(x.shape[1],x.shape[2])
                df2 = pd.DataFrame(x)
                htmldf = pd.concat([htmldf,df2])

            if len(htmldf[htmldf[0].str.contains("Loading") == True]) == 0:
                print("HTML Processing Completed")
            else:
                raise ValueError("HTML Processing Error")

            if len(htmldf.columns) == 7:
                htmldf2 = htmldf.rename({0: "ID", 1: "CPU",2: "Damage",3: "ROF",4: "DPS",5: "Price",6: "Contract"}, axis="columns")

            elif len(htmldf.columns) == 8:
                htmldf2 = htmldf.rename({0: "ID", 1: "CPU",2: "Damage",3: "ROF",4: "DPS",5: "Price",6: "Flag1", 7: "Contract"}, axis="columns")
                flag_df = htmldf2[htmldf2["Contract"].notna()].copy()  
                noflag_df = htmldf2[htmldf2["Contract"].isna()].copy()
                noflag_df["Contract"] = noflag_df["Flag1"]
                noflag_df["Flag1"] = None
                htmldf2 = pd.concat([flag_df, noflag_df], ignore_index=True)

            elif len(htmldf.columns) == 9:
                htmldf2 = htmldf.rename({0: "ID", 1: "CPU",2: "Damage",3: "ROF",4: "DPS",5: "Price",6: "Flag1", 7: "Flag2", 8: "Contract"}, axis="columns")
                two_flag_df = htmldf2[htmldf2["Contract"].notna()].copy()
                one_flag_df = htmldf2[htmldf2["Flag2"].notna() & htmldf2["Contract"].isna()].copy()
                one_flag_df["Contract"] = one_flag_df["Flag2"]
                one_flag_df["Flag2"] = None
                noflag_df = htmldf2[htmldf2["Flag2"].isna()].copy()
                noflag_df["Contract"] = noflag_df["Flag1"]
                noflag_df["Flag1"] = None
                htmldf2 = pd.concat([one_flag_df, two_flag_df], ignore_index=True)
                htmldf2 = pd.concat([htmldf2, noflag_df], ignore_index=True)
                
            else:
                raise ValueError("Invalid html output.")

            htmldf2 = htmldf2.astype('str') 
            htmldf2 = htmldf2.replace("\)","",regex=True).replace("\(","",regex=True).replace("'","",regex=True)
            htmldf2[["trash","ID"]] = htmldf2["ID"].str.split(',',expand=True) 
            htmldf2['ID'] = htmldf2['ID'].str.replace(" ", "")
            htmldf2[["CPU","trash"]] = htmldf2["CPU"].str.split(',',expand=True) 
            htmldf2[["CPU","trash"]] = htmldf2["CPU"].str.split(' ',expand=True) 
            htmldf2[["Damage","trash"]] = htmldf2["Damage"].str.split(',',expand=True) 
            htmldf2[["Damage","trash"]] = htmldf2["Damage"].str.split(' ',expand=True) 
            htmldf2[["ROF","trash"]] = htmldf2["ROF"].str.split(',',expand=True) 
            htmldf2[["ROF","trash"]] = htmldf2["ROF"].str.split(' ',expand=True) 
            htmldf2[["DPS","trash"]] = htmldf2["DPS"].str.split(',',expand=True)  
            htmldf2[["Price","trash"]] = htmldf2["Price"].str.split(',',expand=True) 
            htmldf2[["Price","Unit"]] = htmldf2["Price"].str.split(' ',expand=True) 
            htmldf2["Contract"] = htmldf2["Contract"].str.extract(r'Contract ([0-9]*)')

            if "Flag1" in htmldf2.columns:
                htmldf2[["Flag1","trash"]] = htmldf2["Flag1"].str.split(',',expand=True)
            if "Flag2" in htmldf2.columns:
                htmldf2[["Flag2","trash"]] = htmldf2["Flag2"].str.split(',',expand=True) 

            htmldf2 = htmldf2.drop(columns=["trash"])

            htmldf2["ID"] = "https://mutaplasmid.space" + htmldf2["ID"]

            htmldf2 = htmldf2[htmldf2["ID"].notna()]

            htmldf2.to_csv("./webcrawler/" + str(URL[0]) + "Output.csv",index=False)
            print(str(URL[0]) + "Saved")
            
            t = datetime.datetime.now()
            timestamp = t.strftime("[%d.%m.%y] Time - %H_%M_%S")
            log_msg = str(timestamp)
            print(log_msg)
            with open('./log/' + str(URL[0]) + '.txt','a') as file:
                file.write(log_msg + "\n")

            print(str(URL[0]) + " Log Updated")
            success = True

        except Exception as e:
            print(e)
            attempts += 1
            print(f"Attempt:{attempts}")
            if attempts >= 3:
                print(str(URL[0]) + " Failed")
                break


    


