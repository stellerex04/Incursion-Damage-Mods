import logging

MODS = [
    ['HeatSink', 'Market HeatSinks','49726'],
    ['MagStab', 'Market MagStabs','49722'],
    ['GyroStab', 'Market GyroStabs','49730']
    ] 

COLUMNS = [0,1,2,3, "DPS", "TotalCPU", "Contract_first", "Contract_second", "Contract_third", "Contract_fourth"]

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