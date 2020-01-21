import gzip
import bz2file
import re
import pandas as pd
from tqdm import tqdm_notebook
import numpy as np
import pickle
import geopandas as gpd
import shapely.geometry as geo
from collections import defaultdict
import smtplib
from email import message
import time
import datetime
import csv
import networkx as nx
import os
from shapely.geometry import Point
from functools import partial
import pyproj
from shapely.ops import transform
import collections
import time


original_path = r'C:\Users\Ion\IVT\OSM_python\test'
secondary_path = r'C:\Users\Ion\IVT\OSM_python\test\lie'
# load different files of network
# ----------------------------------------------------------------
# IMPORT nodes_europe4326
file = open(str(original_path) + "/europe_nodes_dict4326.pkl",'rb')
nodes_europe_4326 = pickle.load(file)
file.close()
print('Nnodes in nodes_europe_4326: '+ str(len(nodes_europe_4326)))

file = open(str(original_path) + "/europe_nodes_dict2056.pkl",'rb')
nodes_europe_2056 = pickle.load(file)
file.close()
print('Nnodes in nodes_europe_2056: '+ str(len(nodes_europe_2056)))

# IMPORT nodes from ways to merge
file = open(str(secondary_path) + "/europe_nodes_dict4326.pkl",'rb')
nodes_chsecondary_4326 = pickle.load(file)
file.close()
print('Nnodes in nodes_chsecondary_4326: '+ str(len(nodes_chsecondary_4326)))

file = open(str(secondary_path) + "/europe_nodes_dict2056.pkl",'rb')
nodes_chsecondary_2056 = pickle.load(file)
file.close()
print('Nnodes in nodes_chsecondary_2056: '+ str(len(nodes_chsecondary_2056)))

#IMPORT splitted_ways_dict
file = open(str(original_path) + "/europe_ways_splitted_dict.pkl",'rb')
splitted_ways_dict = pickle.load(file)
file.close()
print('Nways in splitted_ways_dict: '+ str(len(splitted_ways_dict)))

file = open(str(secondary_path) + "/europe_ways_splitted_dict.pkl",'rb')
splitted_secondary_ways_dict  = pickle.load(file)
file.close()
print('Nways in splitted_secondary_ways_dict : '+ str(len(splitted_secondary_ways_dict )))

#IMPORT gdf_MTP_europe.csv
europe_ways_df = pd.read_csv(str(original_path) + "/gdf_MTP_europe.csv", low_memory=False)
print('Nways in europe_ways_df : '+ str(len(europe_ways_df)))

ch_secondary_df = pd.read_csv(str(secondary_path) + "/gdf_MTP_europe.csv")
print('Nways in ch_secondary_df : '+ str(len(ch_secondary_df)))

# IMPORT europe_ways_gdf
europe_ways_gdf = gpd.read_file(str(original_path) + "/gdf_MTP_europe.shp")
print('Nways in europe_ways_gdf: '+ str(len(europe_ways_gdf)))

secondary_ways_gdf = gpd.read_file(str(secondary_path) + "/gdf_MTP_europe.shp")
print('Nways in secondary_ways_gdf: '+ str(len(secondary_ways_gdf)))


# -----------------------------------------------------------------------------
# MERGE NODES
# -----------------------------------------------------------------------------
def merge_nodes(epsg, original_path, secondary_path, out_path):
    print('Transforming nodes of epsg:' + str(epsg))

    # IMPORT nodes_europe
    file = open(str(original_path) + "/europe_nodes_dict" + str(epsg) +".pkl", 'rb')
    nodes_europe = pickle.load(file)
    file.close()
    print('Nnodes in nodes_europe epsg' + str(epsg) + ': ' + str(len(nodes_europe)))

    file = open(str(secondary_path) + "/europe_nodes_dict" + str(epsg) +".pkl", 'rb')
    nodes_chsecondary = pickle.load(file)
    file.close()
    print('Nnodes in nodes_chsecondary epsg' + str(epsg) + ': ' + str(len(nodes_chsecondary)))

    # this will work if dictionary keys=float as keys MUST be sorted
    europe_nodes_merged = {**nodes_europe, **nodes_chsecondary}
    with open(str(out_path) + '/europe_nodes_dict' + str(epsg) + '.pkl', 'wb') as f:
        pickle.dump(europe_nodes_merged, f, pickle.HIGHEST_PROTOCOL)
    print('Nnodes in europe_nodes_dict' + str(epsg) + ': ' + str(len(europe_nodes_4326_merged)))

merge_nodes('2056', original_path, secondary_path, out_path)
merge_nodes('4326', original_path, secondary_path, out_path)

# this will work if dictionary keys=float
# europe_nodes_4326_merged = {**nodes_europe_4326, **nodes_chsecondary_4326}
# with open(str(out_path) + '/europe_nodes_4326_merged' + '.pkl', 'wb') as f:
#     pickle.dump(europe_nodes_4326_merged, f, pickle.HIGHEST_PROTOCOL)
# print('Nnodes in europe_nodes_4326_merged: ' + str(len(europe_nodes_4326_merged)))
#
# # merge nodes_2056
# europe_nodes_2056_merged = {**nodes_europe_2056, **nodes_chsecondary_2056}
# with open(str(out_path) + '/europe_nodes_2056_merged' + '.pkl', 'wb') as f:
#     pickle.dump(europe_nodes_2056_merged, f, pickle.HIGHEST_PROTOCOL)
# print('Nnodes in europe_nodes_2056_merged: ' + str(len(europe_nodes_2056_merged)))

# -----------------------------------------------------------------------------
# MERGE WAYS
# -----------------------------------------------------------------------------
# merge splitted_ways_dicts
# europe_splitted_ways_merged = {**splitted_ways_dict, **splitted_secondary_ways_dict}
# europe_splitted_ways_merged = collections.OrderedDict(sorted(europe_splitted_ways_merged.items()))
list_ways = list(splitted_ways_dict)+list(splitted_secondary_ways_dict)
list_ways.sort(key=float)
europe_splitted_ways_merged = {}
for i in list_ways:
    try:
        europe_splitted_ways_merged[i] = splitted_ways_dict[i]
    except:
        europe_splitted_ways_merged[i] = splitted_secondary_ways_dict[i]
with open(str(out_path) + '\europe_ways_splitted_dict' + '.pkl', 'wb') as f:
    pickle.dump(europe_splitted_ways_merged, f, pickle.HIGHEST_PROTOCOL)
print('Nways in europe_splitted_ways_merged: '+ str(len(europe_splitted_ways_merged)))

europe_ways_merged_df = pd.concat([europe_ways_df,ch_secondary_df])
print(len(europe_ways_df))
print(len(ch_secondary_df))
print(len(europe_ways_merged_df))

europe_ways_merged_df.to_csv(str(out_path) + "\gdf_MTP_europe.csv", sep = ",", index = None)
europe_ways_merged_gdf = pd.concat([europe_ways_gdf,secondary_ways_gdf])
europe_ways_merged_gdf.to_file(str(out_path) + "\gdf_MTP_europe.shp")
print('Files merged succesfully')


create_graph_func(merged_network_path):

# IMPORT G original graph (not only largest island as islands may be connected after merging networks)
# G2=nx.read_gpickle(r"C:\Users\gaion\freight\OSM_python\europe\europe_graph\eu_network_graph_bytime.gpickle")
G2=nx.read_gpickle(r"C:\Users\gaion\freight\OSM_python\switzerland\ch_graph\ch_network_graph_bytime.gpickle")

print('G2 graph (Nnodes/Nedges): '+ str(G2.number_of_nodes()) + '/' + str(G2.number_of_edges()))

#create a graph from network
G = nx.Graph()
# G_isolated = nx.Graph()

train = pd.read_csv(str(out_path) + "\gdf_MTP_europe.csv")
# train = gpd.read_file(str(out_path) + "\gdf_MTP_europe.shp")
edges = train [["start_node_id", "end_node_id", "time(s)", "new_id"]]
edges_list = edges.values.tolist()

for start, end, time, new_id in edges_list:
    G.add_edge(int(start), int(end), time = time, new_id = new_id)
#     G_isolated.add_edge(int(start), int(end), time = time, new_id = new_id)

# merge both graphs G and G2
F = nx.compose(G2,G)
start_Nn=F.number_of_nodes()
# export graph to file
nx.write_gpickle(F, str(out_path) + "\eu_network_graph_bytime.gpickle")

# Identify the largest component and the "isolated" nodes
components = list(nx.connected_components(F)) # list because it returns a generator
components.sort(key=len, reverse=True)
longest_networks = []
for i in range (0,10):
    net = components[i]
    longest_networks.append(len(net))

largest = components.pop(0)
isolated = set(g for cc in components for g in cc)
num_isolated = F.order() - len(largest)

# remove isolated nodes from G
F.remove_nodes_from(isolated)
end_Nn=F.number_of_nodes()

# export GRAPH to file
nx.write_gpickle(F, str(out_path) + "\eu_network_largest_graph_bytime.gpickle")

print('Input edges: '+str(len(edges_list)))
print('N_edges: ' + str(F.number_of_edges()))
print('Start/End N_nodes: ' + str(start_Nn) + '/' + str(end_Nn))

print('N isolated nodes: ' +  str(num_isolated))
print('10 largest networks (nodes): ' + str(longest_networks))