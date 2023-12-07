import logging

MODS = [
    ['HeatSink', 'Market HeatSinks','49726'],
    ['MagStab', 'Market MagStabs','49722'],
    ['GyroStab', 'Market GyroStabs','49730']
    ] 

COLUMNS = [0,1,2,3, "TotalDamage", "TotalROF", "TotalCPU", "Contract_first", "Contract_second", "Contract_third", "Contract_fourth"]

def start_logging(path: str = None):
    return logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(path),
        logging.StreamHandler()  # Add this line to enable logging to console
    ]
)