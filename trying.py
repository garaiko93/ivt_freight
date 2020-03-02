import time
import bz2file
def blocks(files, size=65536):
    while True:
        b = files.read(size)
        if not b: break
        yield b

start_time = time.time()

# file = 'C:/Users/Ion/IVT/OSM_data/liechtenstein-latest.osm.bz2'

with open(file, "r",encoding="utf-8",errors='ignore') as f:
    # for line in f:
    #     print(line)
    print (sum(bl.count("\n") for bl in blocks(f)))
    # print(sum(bl.count(b"<node ") for bl in blocks(f)))
    # print(sum(bl.count("<way ") for bl in blocks(f)))

end_time = time.time()
print(end_time - start_time)

start_time = time.time()
file = 'C:/Users/Ion/IVT/OSM_data/switzerland-latest.osm.bz2'
node_count = 0
way_count = 0
with bz2file.open(str(file)) as f:
    for line in f:
        if b"<node " in line:
            node_count += 1
        if b"<way " in line:
            way_count +=1
print(node_count, way_count)
end_time = time.time()
print(end_time - start_time)


import pickle
import datetime
import igraph as ig
from scipy import spatial
from shapely.geometry import Point
import networkx as nx
import geopandas as gpd
import pandas as pd

def ls_func(lon, lat, name):
    point = Point((lon,lat))
    print(name, end="\r")
    return point

# filename = 'C:/Users/Ion/IVT/OSM_python/networks/eu1234/network_files/nodes_dict_4326.pkl'
filename = '/cluster/home/gaion/freight/networks/eu1234/network_files/europe_nodes_dict4326.pkl'
file = open(filename, 'rb')
nodes_dict = pickle.load(file)
df = pd.DataFrame.from_dict(nodes_dict)
df['geometry'] = df.apply(lambda row: ls_func(row['lon'], row['lat'], row.name), axis=1)

gdf = gpd.GeoDataFrame(df)
gdf.crs = {"epsg:4326"}
gdf = gdf.to_crs({"epsg:2056"})

