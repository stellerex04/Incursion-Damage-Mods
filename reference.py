import logging

MODS = [
    ['HeatSink', 'Market HeatSinks','49726'],
    ['MagStab', 'Market MagStabs','49722'],
    ['GyroStab', 'Market GyroStabs','49730']
    ] 


SHIPS = {"HeatSink":
            {"damage" :{
                "Laser Pulse": 3.6
                ,"Conflag L": 70.8
                ,"Paladin": 2.0
                ,"Mega Pulse Laser II": 1.25
                ,"Surgical Strike": 1.15
                ,"Pulse Laser": 1.1
                ,"Marauders" : 1.25
                ,"LE-1006": 1.06
                }
            ,"rof": {
                "guns": 7875
                ,"Gunnery": 0.9
                ,"Rapid Firing": 0.8
                ,"RF-906": 0.94
                ,"Rig": 0.85
                }
        }
        ,"MagStab":
            {"damage" :{
                "Neutron Blaster Cannon II": 4.41
                ,"Void L": 70.8
                ,"Kronos": 2.0
                ,"Large Hybrid Turret": 1.25
                ,"Surgical Strike": 1.15
                ,"Large Blaster Spec": 1.1
                ,"Gallente Battleship" : 1.25
                ,"LH-1006": 1.06
                }
            ,"rof": {
                "Neutron Blaster Cannon II": 7875
                ,"Gunnery": 0.9
                ,"Rapid Firing": 0.8
                ,"RF-906": 0.94
                ,"Large Hybrid Burst Aerator II": 0.85
                }
            }
        ,"GyroStab":
            {"damage" :{
                "800mm Repeating Cannon II": 3.234
                ,"Hail L": 70.9
                ,"Vargur": 2.0
                ,"Large Projectitle Turret": 1.25
                ,"Surgical Strike": 1.15
                ,"Large Autocannon Spec": 1.1
                ,"Minmatar Battleship" : 1.375
                ,"LP-1006": 1.06
                }
            ,"rof": {
                "800mm Repeating Cannon II": 7875
                ,"Gunnery": 0.9
                ,"Rapid Firing": 0.8
                ,"RF-906": 0.94
                ,"Large Projectitle Burst Aerator II": 0.85
                }
            }
    }

COLUMNS = ["MOD1","MOD2","MOD3","MOD4", "Total Damage", "TotalCPU", "Contract_first", "Contract_second", "Contract_third", "Contract_fourth"]

def start_logging(log_file, logger_name):
    logger = logging.getLogger(logger_name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        file_handler = logging.FileHandler(log_file, mode='w')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        # Stream handler (outputs to console)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        


def price_norm(row):
        if "billion" in row["Unit"]:
            return row["Price"] * 1000000000
        elif "million" in row["Unit"]:
            return row["Price"] * 1000000 
        elif "plex" in row["Unit"]:
            return row["Price"] * 5100000
        else:
            return row["Price"]
        
def price_df_norm(df):
    df["Price"] = df["Price"].astype("float64")
    df["Unit"] = df["Unit"].fillna("plex")
    df["Contract"] = df["Contract"].astype(str)
    df['Price'] = df.apply(lambda row: price_norm(row), axis=1)
    df["Price"] = df["Price"].astype("int64")
    return df