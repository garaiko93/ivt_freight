#import gzip
#import bz2file
#import re
import pandas as pd
#import pandana
#from tqdm import tqdm_notebook
import numpy as np
import pickle
import geopandas as gpd
from shapely.geometry import Point
from shapely.geometry import LineString,LinearRing, MultiPoint
import shapely.geometry as geo
from shapely import ops
from collections import defaultdict
import networkx as nx
import matplotlib.pyplot as plt
# from sklearn.neighbors import KDTree
from shapely.geometry import shape
import fiona
import json
import csv
import geopy
from geopy import geocoders
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="my-application")
from scipy import spatial
from shapely.ops import cascaded_union
from shapely.geometry import Point
from functools import partial
import pyproj
from shapely.ops import transform
import os
import time
from email import message
import smtplib

# MERGE NETWORKS FILES
in_path = r"C:\Users\Ion\IVT\OSM_python"
# in_path = r"C:\Users\gaion\freight\OSM_python"

# check output directory
network = "eu123_ch4"
out_dir = str(network)+ "\\2_routing"
network_path = str(in_path) + "\merged_networks/" + str(network)

if not os.path.exists(str(out_dir)):
    os.makedirs(str(out_dir))
    print('Directory created')
else:
    print('Directory exists, change name of attempt output directory')


# load shape files of NUTS for each year
cols = ['NUTS_ID','LEVL_CODE', 'CNTR_CODE', 'NUTS_NAME', 'FID', 'geometry']
nuts_2016 = gpd.read_file(str(in_path)+"\freight_data\nuts_borders\nuts_borders\ref-nuts-2016-01m.shp\NUTS_RG_01M_2016_4326.shp\NUTS_RG_01M_2016_4326.shp", encoding='utf8')
nuts_2013 = gpd.read_file(str(in_path)+"\freight_data\nuts_borders\nuts_borders\ref-nuts-2013-01m.shp\NUTS_RG_01M_2013_4326.shp\NUTS_RG_01M_2013_4326.shp", encoding='utf8')
nuts_2010 = gpd.read_file(str(in_path)+"\freight_data\nuts_borders\nuts_borders\ref-nuts-2010-01m.shp\NUTS_RG_01M_2010_4326.shp\NUTS_RG_01M_2010_4326.shp", encoding='utf8')
nuts_2006 = gpd.read_file(str(in_path)+"\freight_data\nuts_borders\nuts_borders\ref-nuts-2006-01m.shp\NUTS_RG_01M_2006_4326.shp\NUTS_RG_01M_2006_4326.shp", encoding='utf8')
nuts_2003 = gpd.read_file(str(in_path)+"\freight_data\nuts_borders\nuts_borders\ref-nuts-2003-01m.shp\NUTS_RG_01M_2003_4326.shp\NUTS_RG_01M_2003_4326.shp", encoding='utf8')

nuts_2016 = nuts_2016[cols]
nuts_2013 = nuts_2013[cols]
nuts_2010 = nuts_2010[cols]
nuts_2006 = nuts_2006[cols]
nuts_2003 = nuts_2003[cols]

# merge all nuts from all files keeping the latest version of each one (polygon)
nutsid_bag = []
unique_nuts = []
all_nuts = [nuts_2016, nuts_2013, nuts_2010, nuts_2006, nuts_2003]

for i in range(0, len(all_nuts)):
    nuts_1year = all_nuts[i]
    for j in range(0, len(nuts_1year)):
        nutsid = nuts_1year.iloc[j]['NUTS_ID']
        if nutsid not in nutsid_bag:
            nutsid_bag.append(nutsid)
            unique_nuts.append(list(nuts_1year.iloc[j]))

unique_nuts_gdf = gpd.GeoDataFrame(unique_nuts, columns=cols)

# tranform coordinate system from 4326 to 2056 and export to file
unique_nuts_gdf.crs = {"init": "EPSG:4326"}
unique_nuts_gdf = unique_nuts_gdf.to_crs({"init": "EPSG:2056"})
unique_nuts_gdf.to_file("NUTS_RG_01M_2003to2016_2056.shp", encoding='utf-8')

print(len(unique_nuts_gdf))
print(len(nuts_2016))
print(len(nuts_2013))
print(len(nuts_2010))
print(len(nuts_2006))
print(len(nuts_2003))
unique_nuts_gdf.head()

# IMPORT G graph with largest network
G=nx.read_gpickle(str(network_path)+"\ch_bordercrossings\offdata_bc\eu_network_graph_with_official_bc.gpickle")
print('Graph has: '+str(len([len(c) for c in sorted(nx.connected_components(G),key=len,reverse=True)]))+' island with '
      + str(G.number_of_nodes())+'/'+str(G.number_of_edges())+' (Nnodes/Nedges)')

# # IMPORT nodes_europe
file = open(str(network_path)+"\europe_nodes_dict2056.pkl",'rb')
nodes_europe_2056 = pickle.load(file)
file.close()
print('Nnodes in nodes_europe_2056: '+ str(len(nodes_europe_2056)))

# # IMPORT nodes_europe
file = open(str(network_path)+"\europe_nodes_dict4326.pkl",'rb')
nodes_europe_4326 = pickle.load(file)
file.close()
print('Nnodes in nodes_europe_4326: '+ str(len(nodes_europe_4326)))

# # IMPORT unique_nuts_gdf
# unique_nuts_gdf = gpd.read_file(r"NUTS_RG_01M_2003to2016_2056.shp")
# print('Nnuts in unique_nuts_gdf: '+ str(len(unique_nuts_gdf)))

# IMPORT plz_gdf
# plz_gdf = gpd.read_file(str(in_path) + '\\freight_data\\nuts_borders\\postal_codes\\PLZO_SHP_LV95\\PLZO_PLZ.shp')
# print('Plz in plz_gdf: '+ str(len(plz_gdf)))

# IMPORT K4g19.shp
# K4g19_gdf = gpd.read_file(r"C:\Users\Ion\IVT\OSM_python\switzerland\ch_bordercrossings\gd-b-00.03-875-gg19\ggg_2019-LV95\shp\g1g19.shp", encoding='utf8')
# K4g19_gdf = gpd.read_file(r"C:\Users\gaion\freight\switzerland\ch_bordercrossings\gd-b-00.03-875-gg19\ggg_2019-LV95\shp\g1g19.shp", encoding='utf8')
# print('Nnuts in K4g19_gdf: '+ str(len(K4g19_gdf)))

# # IMPORT nuts_europe
# file = open(str(out_dir)+"\\nuts_europe_dict.pkl",'rb')
# nuts_europe = pickle.load(file)
# file.close()
# print('Nnuts in nuts_europe: '+ str(len(nuts_europe)))

# IMPORT plz_ch
# file = open(str(out_dir)+"\\plz_ch_dict.pkl",'rb')
# plz_ch = pickle.load(file)
# file.close()
# print('Nplz in plz_ch: '+ str(len(plz_ch)))

# load swiss border shapefile
ch_border_shp = gpd.read_file(str(in_path) + "\switzerland\ch_bordercrossings\swiss_border\g2l19.shp")
# ch_border_shp = gpd.read_file(str(in_path) + "\\switzerland\\ch_bordercrossings\\swiss_border\\bci_polygon.shp")
ch_border = ch_border_shp['geometry'][0]
ch_border


# build tree for KDTree nearest neighbours search
#OPTION 3: input only nodes in largest network in G
G_nodes = list(G.nodes)
G_nodes.sort(key=float)
G_lonlat = []
i = 0
node_sel = G_nodes[i]
for id in list(nodes_europe_2056):
    if node_sel == int(id):
        lonlat = nodes_europe_2056[id]
        G_lonlat.append(lonlat)
        if i < len(G_nodes)-1:
            i+=1
            node_sel = G_nodes[i]
print(len(G_lonlat))

# EUROPE DATA
# -------------------------------------------------------------------------------
# CREATE DICTIONARY WITH CENTROID COORDINATES AND CLOSEST NODE IN EUROPE_NETWORK
nuts_europe = {}
tree = spatial.KDTree(G_lonlat)

for i in range(0, len(unique_nuts_gdf)):
    nuts_id = unique_nuts_gdf.iloc[i]['NUTS_ID']
    polygon = unique_nuts_gdf.iloc[i]['geometry']
    centroid = (polygon.centroid.x, polygon.centroid.y)

    # this gives the closest nodes id from the nuts centroid coordinates
    nn = tree.query(centroid)
    coord = G_lonlat[nn[1]]
    closest_node_id = int(list(nodes_europe_2056.keys())[list(nodes_europe_2056.values()).index((coord[0], coord[1]))])

    # stores as dictionary
    nuts_europe[nuts_id] = [centroid, closest_node_id]
    print(i, end="\r")

# EXPORT nuts_centroid_dict TO FILE
with open(str(out_dir) + '\\nuts_europe_dict' + '.pkl', 'wb') as f:
    pickle.dump(nuts_europe, f, pickle.HIGHEST_PROTOCOL)

print(len(nuts_europe))

# CHECKPOINT: load nuts dictionary
file = open(str(network) + '\\nuts_europe_dict.pkl', 'rb')
nuts_europe = pickle.load(file)
file.close()
print('Nnuts in nuts_europe: ' + str(len(nuts_europe)))

# load europe OD matrix ('GQGV_2014_Mikrodaten.csv' file)
od_europe_df = pd.read_csv(str(in_path) + "\\freight_data\\freight\gqgv\\GQGV_2014\\GQGV_2014_Mikrodaten.csv", sep=",")

# select relevant columns from dataframe
od_europesum_df = od_europe_df[
    ['OID', 'ORIGIN', 'DESTINATION', 'BORDER_CROSSING_IN', 'BORDER_CROSSING_OUT', 'KM_PERFORMANCE', 'WEIGHTING_FACTOR',
     'DIVISOR']]
print(len(od_europe_df))
od_europesum_df.head()

# add columns (o_node_id, d_node_id) to dataframe with closest node depending origin and destination NUT
# also creating list of NUTS ('missing_nuts') which are not defined in the dictionary nuts_europe
missing_nuts = []


def od_func_eu(origin, destination, rowname):
    try:
        o_node_id = nuts_europe[origin][1]
    except:
        o_node_id = None
        if origin not in missing_nuts:
            missing_nuts.append(origin)
    try:
        d_node_id = nuts_europe[destination][1]
    except:
        d_node_id = None
        if destination not in missing_nuts:
            missing_nuts.append(destination)

    print(rowname, end="\r")
    return pd.Series([o_node_id, d_node_id])


od_europesum_df[['o_node_id', 'd_node_id']] = od_europesum_df.apply(
    lambda row: od_func_eu(row['ORIGIN'], row['DESTINATION'], row.name), axis=1)

df = pd.DataFrame(data={"missing_nuts": missing_nuts})
df.to_csv(str(out_dir) + "\missing_nuts.csv", sep=',', index=False)

od_europesum_df = pd.DataFrame.dropna(od_europesum_df)  # in case there are missing nuts not defined in the dictionary
od_europesum_df.to_csv(str(out_dir) + "\od_europesum_df.csv", sep=",", index=None)

print(len(od_europesum_df))
print(len(missing_nuts))
od_europesum_df.head()


# Last filter for 2_routing
od_incorrect_DABC = pd.read_csv(str(out_dir) + "\\od_incorrect_DABC.csv", encoding='latin1')
print('Nroutes in od_incorrect_DABC: '+ str(len(od_incorrect_DABC)))
od_incorrect_DABC.head()

# DROP ROWS FROM incorrect dataframe from 1_ROUTING
droprows = []
print(len(od_europesum_df))
for i in range(0,len(od_europesum_df)):
    print (i, end="\r")
    oid = od_europesum_df.iloc[i]['OID']
    if oid in list(od_incorrect_DABC['OID']):
        droprows.append(i)
od_europesum_df=od_europesum_df.drop(od_europesum_df.index[droprows])
print(len(od_europesum_df))
od_europesum_df.to_csv(str(out_dir) + "\od_europesum_df.csv", sep = ",", index = None, encoding='latin1')

# CH DATA
# -------------------------------------------------------------------------------
# Similar process than europe data:
# CREATE DICTIONARY WITH CENTROID COORDINATES AND CLOSEST NODE IN SELECTED NETWORK OF EVERY PLZ
plz_ch = {}
tree = spatial.KDTree(G_lonlat)


def closest_node(centroid, plz_id):
    nn = tree.query(centroid, k=10000)
    in_ch = ch_border.contains(Point(centroid))
    # this condition ensures to find a closest node of the network that is in the same side of the border than the centroid
    if in_ch == True:
        for point in nn[1]:
            coord = G_lonlat[point]
            node_in_ch = ch_border.contains(Point(coord[0], coord[1]))
            if node_in_ch == True:
                closest_node_id = int(
                    list(nodes_europe_2056.keys())[list(nodes_europe_2056.values()).index((coord[0], coord[1]))])
                break
    elif in_ch == False:
        for point in nn[1]:
            coord = G_lonlat[point]
            node_in_ch = ch_border.contains(Point(coord[0], coord[1]))
            if node_in_ch == False:
                closest_node_id = int(
                    list(nodes_europe_2056.keys())[list(nodes_europe_2056.values()).index((coord[0], coord[1]))])
                break
    plz_ch[str(plz_id)] = [centroid, closest_node_id,
                           in_ch]  # string because in the freight data plz are stored as strings, so later to match


for i in range(0, len(plz_gdf)):
    plz_id = plz_gdf.iloc[i]['PLZ']
    if plz_id not in list(plz_ch):
        poly_list = []
        # this searches for different polygons with the same PLZ code, to find the centroid of the mixture of all of them
        for j in range(i, len(plz_gdf)):
            if plz_gdf.iloc[j]['PLZ'] == plz_id:
                polygon = plz_gdf.iloc[j]['geometry']
                poly_list.append(polygon)

    boundary = gpd.GeoSeries(cascaded_union(poly_list))
    centroid = boundary[0].centroid.coords[0]
    funct_sol = closest_node(centroid, plz_id)
    print(i, end="\r")
# EXPORT nuts_centroid_dict TO FILE
with open(str(out_dir) + '\plz_ch_dict' + '.pkl', 'wb') as f:
    pickle.dump(plz_ch, f, pickle.HIGHEST_PROTOCOL)

print(len(plz_ch))
# plz_ch

#JOURNEYCH data
od_ch_df = pd.read_csv(str(in_path)+'\\freight_data\\freight\\gte\\GTE_2017\\Donnees\\journeych.csv',sep = ";",low_memory=False)
# selects relevant columns from the od matrix
od_chsum_df = od_ch_df [['ernr','fromPlz', 'toPlz', 'fromNuts', 'toNuts']]
for i in range(0,len(od_chsum_df)):
    od_chsum_df.set_value(i, 'fromPlz', od_chsum_df.iloc[i]['fromPlz'].rstrip())
    od_chsum_df.set_value(i, 'toPlz', od_chsum_df.iloc[i]['toPlz'].rstrip())
#     od_chsum_df.at[i, 'fromPlz'] = od_chsum_df.iloc[i]['fromPlz'].rstrip() #in case set_value gets removed from pandas
#     od_chsum_df.at[i, 'toPlz'] = od_chsum_df.iloc[i]['toPlz'].rstrip() #in case set_value gets removed from pandas
#     od_chsum_df.iloc[i]['fromPlz'] = od_chsum_df.iloc[i]['fromPlz'].rstrip() #TOO SLOW
#     od_chsum_df.iloc[i]['toPlz'] = od_chsum_df.iloc[i]['toPlz'].rstrip() #TOO SLOW
    print (i, end="\r")
print(len(od_chsum_df))
print(od_ch_df.columns)
od_chsum_df.head()

# THIS ADDS THE GROSSINGFACTOR TO THE MANIPULATED DATA from switzerland
od_chw_df = pd.read_csv(str(in_path)+'\\freight_data\\freight\\gte\\GTE_2017\\Donnees\\week.csv',sep = ";",low_memory=False)
od_chwsum_df = od_chw_df [['ernr','grossingFactor']]
od_chsum_df=od_chsum_df.merge(od_chwsum_df)
od_chsum_df.head()

# add columns (o_node_id, d_node_id) to dataframe with closest node depending origin and destination PLZ
# also creating list of PLZs ('missing_plz') which are not defined in the dictionary plz_ch
missing_plz = []


def od_func_ch(origin, destination, rowname):
    try:
        o_node_id = int(plz_ch[origin][1])
    except:
        o_node_id = None
        if origin not in missing_plz:
            missing_plz.append(origin)
    try:
        d_node_id = int(plz_ch[destination][1])
    except:
        d_node_id = None
        if destination not in missing_plz:
            missing_plz.append(destination)

    print(rowname, end="\r")
    return pd.Series([(o_node_id), d_node_id])


od_chsum_df[['o_node_id', 'd_node_id']] = od_chsum_df.apply(
    lambda row: od_func_ch(row['fromPlz'], row['toPlz'], row.name), axis=1)

df = pd.DataFrame(data={"missing_plz": missing_plz})
df.to_csv("./missing_plz.csv", sep=',', index=False)

od_chsum_df = pd.DataFrame.dropna(od_chsum_df)  # in case there are missing nuts not defined in the dictionary
od_chsum_df.to_csv(str(out_dir) + "\od_chsum_df.csv", sep=",", index=None)

print(len(od_chsum_df))
print(len(missing_plz))
od_chsum_df.head()