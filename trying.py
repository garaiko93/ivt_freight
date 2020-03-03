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

filename = r'C:/Users/Ion/IVT/OSM_python/networks/eu123/network_files/europe_nodes_dict4326.pkl'
# filename = '/cluster/home/gaion/freight/networks/eu1234/network_files/europe_nodes_dict4326.pkl'
file = open(filename, 'rb')
nodes_dict = pickle.load(file)
newdict = {}
for node in list(nodes_dict):
    newdict[node] = (nodes_dict[node][1], nodes_dict[node][0])

with open(r'C:/Users/Ion/IVT/OSM_python/networks/eu123/network_files/europe_nodes_dict43262.pkl', 'wb') as f:
    pickle.dump(newdict, f, pickle.HIGHEST_PROTOCOL)

df = pd.DataFrame.from_dict(nodes_dict)
df['geometry'] = df.apply(lambda row: ls_func(row['lon'], row['lat'], row.name), axis=1)

gdf = gpd.GeoDataFrame(df)
gdf.crs = {"epsg:4326"}
gdf = gdf.to_crs({"epsg:2056"})

import geopandas as gpd

shp_file=r'C:/Users/Ion/IVT/OSM_python/switzerland/ch_bordercrossings/swiss_border/bci_polygon30k_4326.shp'
ch_border30k = gpd.read_file(str(shp_file))
# ch_b = ch_border30k.iloc[0]['geometry']

# ch_border30k.crs = {'init': 'epsg:4326'}
ch_border30k = ch_border30k.to_crs("epsg:2056")
ch_border30k.crs
ch_border30k

y,x = 2455975.808, 1115474.254
x,y = 47.398466, 8.562226
point = Point(x,y)

ch_border30k.contains(Point(y, x))
ch_b.contains(Point(y, x))
