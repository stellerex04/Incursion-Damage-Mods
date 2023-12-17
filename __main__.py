import webcrawl
from matching_mods import matching_mods
from surfacedata import surface_data
import surfacedata
from reference import MODS

if __name__ == '__main__':
    try:
        webcrawl
        matching_mods(MODS[0], 4, 23, 800000000)
        matching_mods(MODS[1], 4, 23, 800000000)
        matching_mods(MODS[1], 3, 24, 800000000)
        matching_mods(MODS[2], 4, 25, 800000000)
        surface_data(MODS[0], 4)
        surface_data(MODS[1], 4)
        surface_data(MODS[1], 3)
        surface_data(MODS[2], 4)
    except Exception as e:
        raise ValueError(e)