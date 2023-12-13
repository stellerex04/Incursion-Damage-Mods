import webcrawl
import matching_mods
import surfacedata

if __name__ == '__main__':
    try:
        webcrawl
        matching_mods
        surfacedata
    except Exception as e:
        raise ValueError(e)