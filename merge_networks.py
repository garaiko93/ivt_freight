import pandas as pd
import pickle
import os
import geopandas as gpd
import networkx as nx
import datetime
from create_graph import create_graph_func


def merge_networks_funct(original_path, secondary_path, out_path):
    print(datetime.datetime.now(), 'Merging networks process begins ...')
# original_path = r'C:/Users/Ion/IVT/OSM_python/test/lie/MTP'
# secondary_path = r'C:/Users/Ion/IVT/OSM_python/test/lie/S'
# out_path = r'C:/Users/Ion/IVT/OSM_python/test/lie/merged/network'
    out_path = str(out_path) + '/network_files'
    original_path = str(original_path) + '/network_files'
    secondary_path = str(secondary_path) + '/network_files'

    if not os.path.exists(str(out_path)):
        os.makedirs(str(out_path))
        print(datetime.datetime.now(), 'Directory created')
    else:
        print(datetime.datetime.now(), 'Directory exists')
# -----------------------------------------------------------------------------
# MERGE NODES
# -----------------------------------------------------------------------------
    def merge_nodes(epsg, original_path, secondary_path, out_path):
        print(datetime.datetime.now(), 'Merging nodes of epsg:' + str(epsg) + ' ...')

        # IMPORT nodes_europe
        file = open(str(original_path) + "/europe_nodes_dict" + str(epsg) +".pkl", 'rb')
        nodes_europe = pickle.load(file)
        file.close()
        print(datetime.datetime.now(), 'Nnodes in nodes_europe epsg' + str(epsg) + ': ' + str(len(nodes_europe)))

        file = open(str(secondary_path) + "/europe_nodes_dict" + str(epsg) +".pkl", 'rb')
        nodes_chsecondary = pickle.load(file)
        file.close()
        print(datetime.datetime.now(), 'Nnodes in nodes_chsecondary epsg' + str(epsg) + ': ' + str(len(nodes_chsecondary)))

        # this will work if dictionary keys=float as keys MUST be sorted
        europe_nodes_merged = {**nodes_europe, **nodes_chsecondary}
        with open(str(out_path) + '/europe_nodes_dict' + str(epsg) + '.pkl', 'wb') as f:
            pickle.dump(europe_nodes_merged, f, pickle.HIGHEST_PROTOCOL)
        print(datetime.datetime.now(), 'Nnodes in europe_nodes_dict' + str(epsg) + ': ' + str(len(europe_nodes_merged)))
        print('------------------------------------------------------------------------')
        return europe_nodes_merged

    if os.path.isfile(str(out_path) + "/europe_nodes_dict2056.pkl") == False:
        nodes_dict2056 = merge_nodes('2056', original_path, secondary_path, out_path)
    else:
        print(datetime.datetime.now(), 'Nodes dictionary with epsg 2056 was already merged before.')
        print('------------------------------------------------------------------------')

    if os.path.isfile(str(out_path) + "/europe_nodes_dict4326.pkl") == False:
        nodes_dict4326 = merge_nodes('4326', original_path, secondary_path, out_path)
    else:
        print(datetime.datetime.now(), 'Nodes dictionary with epsg 4326 was already merged before.')
        print('------------------------------------------------------------------------')

# -----------------------------------------------------------------------------
# MERGE WAYS
# -----------------------------------------------------------------------------
    if os.path.isfile(str(out_path) + "/europe_ways_splitted_dict.pkl") == False:
        print(datetime.datetime.now(), 'Merging splitted ways dictionaries ...')
        #IMPORT splitted_ways_dict
        file = open(str(original_path) + "/europe_ways_splitted_dict.pkl",'rb')
        splitted_ways_dict = pickle.load(file)
        file.close()
        print(datetime.datetime.now(), 'Nways in splitted_ways_dict: '+ str(len(splitted_ways_dict)))

        file = open(str(secondary_path) + "/europe_ways_splitted_dict.pkl",'rb')
        splitted_secondary_ways_dict  = pickle.load(file)
        file.close()
        print(datetime.datetime.now(), 'Nways in splitted_secondary_ways_dict : '+ str(len(splitted_secondary_ways_dict )))
        # merge splitted_ways_dicts

        europe_splitted_ways_merged = {**splitted_ways_dict, **splitted_secondary_ways_dict}
        with open(str(out_path) + '/europe_ways_splitted_dict' + '.pkl', 'wb') as f:
            pickle.dump(europe_splitted_ways_merged, f, pickle.HIGHEST_PROTOCOL)
        print(datetime.datetime.now(), 'Nways in europe_splitted_ways_merged: '+ str(len(europe_splitted_ways_merged)))
        print('------------------------------------------------------------------------')
    else:
        print(datetime.datetime.now(), 'Splitted ways was already merged before.')
        print('------------------------------------------------------------------------')

    # europe_splitted_ways_merged = collections.OrderedDict(sorted(europe_splitted_ways_merged.items()))
    # list_ways = list(splitted_ways_dict)+list(splitted_secondary_ways_dict)
    # list_ways.sort(key=float)
    # europe_splitted_ways_merged = {}
    # for i in list_ways:
    #     try:
    #         europe_splitted_ways_merged[i] = splitted_ways_dict[i]
    #     except:
    #         europe_splitted_ways_merged[i] = splitted_secondary_ways_dict[i]
    # with open(str(out_path) + '\europe_ways_splitted_dict' + '.pkl', 'wb') as f:
    #     pickle.dump(europe_splitted_ways_merged, f, pickle.HIGHEST_PROTOCOL)
    # print('Nways in europe_splitted_ways_merged: '+ str(len(europe_splitted_ways_merged2)))

# -----------------------------------------------------------------------------
# MERGE WAYS CSV
# -----------------------------------------------------------------------------
    if os.path.isfile(str(out_path) + "/gdf_MTP_europe.csv") == False:
        print(datetime.datetime.now(), 'Merging ways.csv ...')
        # IMPORT gdf_MTP_europe.csv
        europe_ways_df = pd.read_csv(str(original_path) + "/gdf_MTP_europe.csv", low_memory=False)
        print(datetime.datetime.now(), 'Nways in europe_ways_df : ' + str(len(europe_ways_df)))
        ch_secondary_df = pd.read_csv(str(secondary_path) + "/gdf_MTP_europe.csv")
        print(datetime.datetime.now(), 'Nways in ch_secondary_df : ' + str(len(ch_secondary_df)))

        europe_ways_merged_df = pd.concat([europe_ways_df, ch_secondary_df])
        europe_ways_merged_df.to_csv(str(out_path) + "/gdf_MTP_europe.csv", sep=",", index=None)
        print(datetime.datetime.now(), 'Nways in europe_ways_merged_df: ' + str(len(europe_ways_merged_df)))
        print('------------------------------------------------------------------------')
    else:
        print(datetime.datetime.now(), 'Ways CSV was already merged before.')
        print('------------------------------------------------------------------------')

# -----------------------------------------------------------------------------
# MERGE WAYS SHP
# -----------------------------------------------------------------------------
    if os.path.isfile(str(out_path) + "/gdf_MTP_europe.shp") == False:
        print(datetime.datetime.now(), 'Merging ways shp file ...')
        # IMPORT europe_ways_gdf
        europe_ways_gdf = gpd.read_file(str(original_path) + "/gdf_MTP_europe.shp")
        print(datetime.datetime.now(), 'Nways in europe_ways_gdf: ' + str(len(europe_ways_gdf)))

        secondary_ways_gdf = gpd.read_file(str(secondary_path) + "/gdf_MTP_europe.shp")
        print(datetime.datetime.now(), 'Nways in secondary_ways_gdf: ' + str(len(secondary_ways_gdf)))

        europe_ways_merged_gdf = pd.concat([europe_ways_gdf,secondary_ways_gdf], sort=True)
        europe_ways_merged_gdf.to_file(str(out_path) + "/gdf_MTP_europe.shp")
        print(datetime.datetime.now(), 'Nways in europe_ways_merged_gdf: ' + str(len(europe_ways_merged_gdf)))
        print('------------------------------------------------------------------------')
    else:
        print(datetime.datetime.now(), 'Ways SHP was already merged before.')
        print('------------------------------------------------------------------------')


# -----------------------------------------------------------------------------
# MERGE GRAPHS
# -----------------------------------------------------------------------------
    if os.path.isfile(str(out_path) + "/eu_network_largest_graph_bytime.gpickle") == False:
        print(datetime.datetime.now(), 'Merging graph ...')
        # load data
        file = open(str(out_path) + "/europe_nodes_dict2056.pkl", 'rb')
        nodes_dict2056 = pickle.load(file)
        file.close()
        print(datetime.datetime.now(), 'Nnodes in nodes_europe epsg2056: ' + str(len(nodes_dict2056)))

        europe_ways_merged_df = pd.read_csv(str(out_path) + "/gdf_MTP_europe.csv", low_memory=False)

        file = open(str(out_path) + "/europe_ways_splitted_dict.pkl", 'rb')
        europe_splitted_ways_merged = pickle.load(file)
        file.close()
        print(datetime.datetime.now(), 'Nways in splitted_ways_dict: ' + str(len(europe_splitted_ways_merged)))


        # This will create a graph from scratch with the merged dataframes
        create_graph_func(out_path, europe_ways_merged_df, nodes_dict2056, europe_splitted_ways_merged)

        print(datetime.datetime.now(), 'Merging of networks finished successfully, files in out_path')
        print('------------------------------------------------------------------------')
