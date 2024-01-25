import webcrawl
from matching_mods import matching_mods
from surfacedata import surface_data
import surfacedata
from reference import MODS

if __name__ == '__main__':
    try:
        webcrawl
        matching_mods(MODS[0], 4, 23, 1500000000)
        matching_mods(MODS[0], 4, 24, 2000000000)
        matching_mods(MODS[0], 4, 25, 3000000000)
        surface_data(MODS[0], 4, targets=[[3350, "_DPS_23_PRICE_1500000000"], [3400, "_DPS_24_PRICE_2000000000"], [3450, "_DPS_25_PRICE_3000000000"]])

        matching_mods(MODS[1], 4, 23, 1000000000)
        matching_mods(MODS[1], 4, 24, 1500000000)
        matching_mods(MODS[1], 4, 25, 3000000000)
        surface_data(MODS[1], 4, targets=[[4100, "_DPS_23_PRICE_1000000000"], [4150, "_DPS_24_PRICE_1500000000"], [4200, "_DPS_25_PRICE_3000000000"]])

        matching_mods(MODS[1], 3, 24, 1000000000)
        matching_mods(MODS[1], 3, 27, 2000000000)
        matching_mods(MODS[1], 3, 28, 3000000000)
        surface_data(MODS[1], 3, targets=[[3900, "_DPS_24_PRICE_1000000000"], [3950, "_DPS_27_PRICE_2000000000"], [4000, "_DPS_28_PRICE_3000000000"]])
      
        matching_mods(MODS[2], 4, 25, 1000000000)
        matching_mods(MODS[2], 4, 26, 2000000000)
        matching_mods(MODS[2], 4, 28, 5000000000)
        surface_data(MODS[2], 4, targets=[[3350, "_DPS_25_PRICE_1000000000"], [3400, "_DPS_26_PRICE_2000000000"], [3450, "_DPS_28_PRICE_5000000000"]])
    except Exception as e:
        raise ValueError(e)