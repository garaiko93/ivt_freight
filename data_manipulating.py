import pandas as pd
import pickle
import geopandas as gpd
import datetime
import networkx as nx
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="my-application")
from scipy import spatial
from shapely.ops import cascaded_union
from shapely.geometry import Point
import os
from progressbar import Percentage, ProgressBar, Bar, ETA

# STANDARD PATHS:
# network_path = r"C:/Users/Ion/IVT/OSM_python/merged_networks/eu123_ch4/network"
# out_path = str(network_path) + '/freight_data'
# # data paths
# nuts_path = r"C:/Users/Ion/IVT/OSM_python/freight_data/nuts_borders/nuts_borders"
# graph_path = str(network_path) + '/bc_official/eu_network_graph_with_official_bc.gpickle'
# border_poly_path = r"C:/Users/Ion/IVT/OSM_python/\switzerland/ch_bordercrossings/swiss_border/g2l19.shp"
# europe_data_path = r"C:/Users/Ion/IVT/OSM_python/freight_data/freight/gqgv/GQGV_2014/GQGV_2014_Mikrodaten.csv"




# paths to define:
# out_path = r"C:/Users/Ion/IVT/OSM_python/test/freight_data"
# nuts_path = r"C:/Users/Ion/IVT/OSM_python/freight_data/nuts_borders/nuts_borders"
# network_path = r"C:/Users/Ion/IVT/OSM_python/merged_networks/eu123_ch4"
# # out_path = str(network_path) + '/routing'
#
# # data paths
# graph_path = r"C:/Users/Ion/IVT/OSM_python/merged_networks/eu123_ch4/ch_bordercrossings/offdata_bc"
# border_poly_path = r"C:/Users/Ion/IVT/OSM_python/\switzerland/ch_bordercrossings/swiss_border"
# europe_data_path = r"C:/Users/Ion/IVT/OSM_python/freight_data/freight/gqgv/GQGV_2014/GQGV_2014_Mikrodaten.csv"



# -----------------------------------------------------------------------------
# LOAD NECESSARY FILES FOR DATA MANIPULATION
# -----------------------------------------------------------------------------

# IMPORT G graph with largest network
# G = nx.read_gpickle(str(graph_path))
# print(datetime.datetime.now(), 'Graph has: '+str(len([len(c) for c in sorted(nx.connected_components(G), key=len, reverse=True)]))+' island with '
#       + str(G.number_of_nodes())+'/'+str(G.number_of_edges())+' (Nnodes/Nedges)')
#
# # # IMPORT nodes_europe
# file = open(str(network_path)+"/europe_nodes_dict2056.pkl", 'rb')
# nodes_europe_2056 = pickle.load(file)
# file.close()
# print(datetime.datetime.now(), 'Nnodes in nodes_europe_2056: ' + str(len(nodes_europe_2056)))
#
# # # IMPORT nodes_europe
# file = open(str(network_path)+"/europe_nodes_dict4326.pkl", 'rb')
# nodes_europe_4326 = pickle.load(file)
# file.close()
# print(datetime.datetime.now(), 'Nnodes in nodes_europe_4326: ' + str(len(nodes_europe_4326)))

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
# ch_border_shp = gpd.read_file(str(border_poly_path))
# ch_border_shp = gpd.read_file(str(in_path) + "\switzerland\ch_bordercrossings\swiss_border\g2l19.shp")
# ch_border_shp = gpd.read_file(str(in_path) + "\\switzerland\\ch_bordercrossings\\swiss_border\\bci_polygon.shp")
# ch_border = ch_border_shp['geometry'][0]
# ch_border


def nuts_merging(nuts_path):
    # load shape files of NUTS for each year
    cols = ['NUTS_ID', 'LEVL_CODE', 'CNTR_CODE', 'NUTS_NAME', 'FID', 'geometry']
    nuts_2016 = gpd.read_file(
        str(nuts_path) + "/ref-nuts-2016-01m.shp/NUTS_RG_01M_2016_4326.shp/NUTS_RG_01M_2016_4326.shp",
        encoding='utf8')
    nuts_2013 = gpd.read_file(
        str(nuts_path) + "/ref-nuts-2013-01m.shp/NUTS_RG_01M_2013_4326.shp/NUTS_RG_01M_2013_4326.shp",
        encoding='utf8')
    nuts_2010 = gpd.read_file(
        str(nuts_path) + "/ref-nuts-2010-01m.shp/NUTS_RG_01M_2010_4326.shp/NUTS_RG_01M_2010_4326.shp",
        encoding='utf8')
    nuts_2006 = gpd.read_file(
        str(nuts_path) + "/ref-nuts-2006-01m.shp/NUTS_RG_01M_2006_4326.shp/NUTS_RG_01M_2006_4326.shp",
        encoding='utf8')
    nuts_2003 = gpd.read_file(
        str(nuts_path) + "/ref-nuts-2003-01m.shp/NUTS_RG_01M_2003_4326.shp/NUTS_RG_01M_2003_4326.shp",
        encoding='utf8')

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
    # unique_nuts_gdf.crs = {"epsg:4326"}
    # unique_nuts_gdf = unique_nuts_gdf.to_crs("epsg:2056")
    # unique_nuts_gdf.to_file(str(out_path) + "/NUTS_RG_01M_2003to2016_2056.shp", encoding='utf-8')
    # unique_nuts_gdf.to_file(str(out_path) + "/NUTS_RG_01M_2003to2016_2056.shp")

    print(datetime.datetime.now(), 'After merging the number of defined NUTS is: ' + str(len(unique_nuts_gdf)))
    return unique_nuts_gdf
# -----------------------------------------------------------------------------
# EUROPE DATA
# -----------------------------------------------------------------------------
# CREATE DICTIONARY WITH CENTROID COORDINATES AND CLOSEST NODE IN EUROPE_NETWORK


def europe_data(network_objects, network_path, nuts_path, europe_data_path):
    print(datetime.datetime.now(), 'Europe data manipulating begins ...')
    if network_path is not None:
        out_path = str(network_path) + '/freight_data'
        network_files = str(network_path) + '/network_files'
        graph_path = str(network_path) + '/bc_official/eu_network_graph_with_official_bc.gpickle'

        if not os.path.exists(str(out_path)):
            os.makedirs(str(out_path))
            print(datetime.datetime.now(), 'Directory created.')
        else:
            print(datetime.datetime.now(), 'Directory exists.')

        # Create dictionary with nuts id and coordinates of centroid and closest node id to it
        # IMPORT G graph with largest network
        G = nx.read_gpickle(str(graph_path))

        # # IMPORT nodes_europe
        # file = open(str(network_files) + "/europe_nodes_dict2056.pkl", 'rb')
        file = open(str(network_files) + "/europe_nodes_dict4326.pkl", 'rb')
        nodes_europe_2056 = pickle.load(file)
        file.close()
    else:
        out_path = None
        G = network_objects[0]
        nodes_europe_2056 = network_objects[2]

    print(datetime.datetime.now(), 'Graph has: ' + str(
        len([len(c) for c in sorted(nx.connected_components(G), key=len, reverse=True)])) + ' island with '
          + str(G.number_of_nodes()) + '/' + str(G.number_of_edges()) + ' (Nnodes/Nedges)')
    print(datetime.datetime.now(), 'Nnodes in nodes_europe_2056: ' + str(len(nodes_europe_2056)))


    if os.path.isfile(str(out_path) + '/nuts_europe_dict.pkl') is False or out_path is None:
        # Merge data from NUTS into one gdf:
        unique_nuts_gdf = nuts_merging(nuts_path)

        # Build tree for KDTree nearest neighbours search, in G only start and end nodes are included
        # OPTION 3: input only nodes in largest network in G
        G_nodes = list(G.nodes)
        G_nodes.sort(key=float)
        G_lonlat = []
        i = 0
        node_sel = G_nodes[i]
        for id in list(nodes_europe_2056):
            if node_sel == int(id):
                lonlat = nodes_europe_2056[id]
                G_lonlat.append(lonlat)
                if i < len(G_nodes) - 1:
                    i += 1
                    node_sel = G_nodes[i]
        print(datetime.datetime.now(), 'KDTree has: ' + str(len(G_lonlat)) + ' nodes.')
        print('------------------------------------------------------------------------')

        nuts_europe = {}
        tree = spatial.KDTree(G_lonlat)
        pbar = ProgressBar(widgets=[Bar('>', '[', ']'), ' ',
                                            Percentage(), ' ',
                                            ETA()], maxval=len(unique_nuts_gdf))
        for i in pbar(range(len(unique_nuts_gdf))):
            nuts_id = unique_nuts_gdf.iloc[i]['NUTS_ID']
            polygon = unique_nuts_gdf.iloc[i]['geometry']
            centroid = (polygon.centroid.x, polygon.centroid.y)

            # this gives the closest nodes id from the nuts centroid coordinates
            nn = tree.query(centroid)
            coord = G_lonlat[nn[1]]
            closest_node_id = int(list(nodes_europe_2056.keys())[list(nodes_europe_2056.values()).index((coord[0], coord[1]))])

            # stores as dictionary
            nuts_europe[nuts_id] = [centroid, closest_node_id]
            # print(datetime.datetime.now(), i, end="\r")

        # EXPORT nuts_centroid_dict TO FILE
        if out_path is not None:
            with open(str(out_path) + '/nuts_europe_dict' + '.pkl', 'wb') as f:
                pickle.dump(nuts_europe, f, pickle.HIGHEST_PROTOCOL)

        print(datetime.datetime.now(), len(nuts_europe))
        print('------------------------------------------------------------------------')
    else:
        # CHECKPOINT: load nuts dictionary
        file = open(str(out_path) + '/nuts_europe_dict.pkl', 'rb')
        nuts_europe = pickle.load(file)
        file.close()
        print(datetime.datetime.now(), 'Nnuts in nuts_europe: ' + str(len(nuts_europe)))
        print('------------------------------------------------------------------------')

    # load europe OD matrix ('GQGV_2014_Mikrodaten.csv' file)
    od_europe_df = pd.read_csv(str(europe_data_path), sep=",")

    # select relevant columns from dataframe
    od_europesum_df = od_europe_df[
        ['OID', 'ORIGIN', 'DESTINATION', 'BORDER_CROSSING_IN', 'BORDER_CROSSING_OUT', 'KM_PERFORMANCE', 'WEIGHTING_FACTOR',
         'DIVISOR']]

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

        # print(datetime.datetime.now(), rowname, end="\r")
        return pd.Series([o_node_id, d_node_id])

    od_europesum_df[['o_node_id', 'd_node_id']] = od_europesum_df.apply(
        lambda row: od_func_eu(row['ORIGIN'], row['DESTINATION'], row.name), axis=1)

    df = pd.DataFrame(data={"missing_nuts": missing_nuts})
    od_europesum_df = pd.DataFrame.dropna(
    od_europesum_df)  # in case there are missing nuts not defined in the dictionary

    if out_path is not None:
        df.to_csv(str(out_path) + "/missing_nuts.csv", sep=',', index=False)
        od_europesum_df.to_csv(str(out_path) + "/od_europesum_df.csv", sep=",", index=None)

    print(datetime.datetime.now(), 'Process of manipulating europa data finished')
    print('------------------------------------------------------------------------')

    # Last filter for 2_routing
    if os.path.isfile(str(out_path) + "/od_incorrect_DABC.csv") == True:
        od_incorrect_DABC = pd.read_csv(str(out_path) + "/od_incorrect_DABC.csv", encoding='latin1')
        print('Nroutes in od_incorrect_DABC: '+ str(len(od_incorrect_DABC)))
        od_incorrect_DABC.head()

        # DROP ROWS FROM incorrect dataframe from 1_ROUTING
        droprows = []
        print(len(od_europesum_df))
        for i in range(len(od_europesum_df)):
            print(i, end="\r")
            oid = od_europesum_df.iloc[i]['OID']
            if oid in list(od_incorrect_DABC['OID']):
                droprows.append(i)
        od_europesum_df=od_europesum_df.drop(od_europesum_df.index[droprows])
        od_europesum_df.to_csv(str(out_path) + "/od_europesum_df.csv", sep = ",", index = None, encoding='latin1')

        print(len(od_europesum_df))
        print('------------------------------------------------------------------------')



# CH DATA
# -------------------------------------------------------------------------------
# Similar process than europe data:
# CREATE DICTIONARY WITH CENTROID COORDINATES AND CLOSEST NODE IN SELECTED NETWORK OF EVERY PLZ
# plz_ch = {}
# tree = spatial.KDTree(G_lonlat)
#
# def closest_node(centroid, plz_id):
#     nn = tree.query(centroid, k=10000)
#     in_ch = ch_border.contains(Point(centroid))
#     # this condition ensures to find a closest node of the network that is in the same side of the border than the centroid
#     if in_ch == True:
#         for point in nn[1]:
#             coord = G_lonlat[point]
#             node_in_ch = ch_border.contains(Point(coord[0], coord[1]))
#             if node_in_ch == True:
#                 closest_node_id = int(
#                     list(nodes_europe_2056.keys())[list(nodes_europe_2056.values()).index((coord[0], coord[1]))])
#                 break
#     elif in_ch == False:
#         for point in nn[1]:
#             coord = G_lonlat[point]
#             node_in_ch = ch_border.contains(Point(coord[0], coord[1]))
#             if node_in_ch == False:
#                 closest_node_id = int(
#                     list(nodes_europe_2056.keys())[list(nodes_europe_2056.values()).index((coord[0], coord[1]))])
#                 break
#     plz_ch[str(plz_id)] = [centroid, closest_node_id,
#                            in_ch]  # string because in the freight data plz are stored as strings, so later to match
#
# for i in range(0, len(plz_gdf)):
#     plz_id = plz_gdf.iloc[i]['PLZ']
#     if plz_id not in list(plz_ch):
#         poly_list = []
#         # this searches for different polygons with the same PLZ code, to find the centroid of the mixture of all of them
#         for j in range(i, len(plz_gdf)):
#             if plz_gdf.iloc[j]['PLZ'] == plz_id:
#                 polygon = plz_gdf.iloc[j]['geometry']
#                 poly_list.append(polygon)
#
#     boundary = gpd.GeoSeries(cascaded_union(poly_list))
#     centroid = boundary[0].centroid.coords[0]
#     funct_sol = closest_node(centroid, plz_id)
#     print(i, end="\r")
# # EXPORT nuts_centroid_dict TO FILE
# with open(str(out_path) + '\plz_ch_dict' + '.pkl', 'wb') as f:
#     pickle.dump(plz_ch, f, pickle.HIGHEST_PROTOCOL)
#
# print(len(plz_ch))
# # plz_ch
#
# #JOURNEYCH data
# od_ch_df = pd.read_csv(str(in_path)+'/freight_data/freight/gte/GTE_2017/Donnees/journeych.csv', sep=";",
#                        low_memory=False)
# # selects relevant columns from the od matrix
# od_chsum_df = od_ch_df [['ernr','fromPlz', 'toPlz', 'fromNuts', 'toNuts']]
# for i in range(0,len(od_chsum_df)):
#     od_chsum_df.set_value(i, 'fromPlz', od_chsum_df.iloc[i]['fromPlz'].rstrip())
#     od_chsum_df.set_value(i, 'toPlz', od_chsum_df.iloc[i]['toPlz'].rstrip())
# #     od_chsum_df.at[i, 'fromPlz'] = od_chsum_df.iloc[i]['fromPlz'].rstrip() #in case set_value gets removed from pandas
# #     od_chsum_df.at[i, 'toPlz'] = od_chsum_df.iloc[i]['toPlz'].rstrip() #in case set_value gets removed from pandas
# #     od_chsum_df.iloc[i]['fromPlz'] = od_chsum_df.iloc[i]['fromPlz'].rstrip() #TOO SLOW
# #     od_chsum_df.iloc[i]['toPlz'] = od_chsum_df.iloc[i]['toPlz'].rstrip() #TOO SLOW
#     print (i, end="\r")
#
# # THIS ADDS THE GROSSINGFACTOR TO THE MANIPULATED DATA from switzerland
# od_chw_df = pd.read_csv(str(in_path)+'/freight_data/freight/gte/GTE_2017/Donnees/week.csv', sep=";", low_memory=False)
# od_chwsum_df = od_chw_df [['ernr','grossingFactor']]
# od_chsum_df=od_chsum_df.merge(od_chwsum_df)
# od_chsum_df.head()
#
# # add columns (o_node_id, d_node_id) to dataframe with closest node depending origin and destination PLZ
# # also creating list of PLZs ('missing_plz') which are not defined in the dictionary plz_ch
# missing_plz = []
#
#
# def od_func_ch(origin, destination, rowname):
#     try:
#         o_node_id = int(plz_ch[origin][1])
#     except:
#         o_node_id = None
#         if origin not in missing_plz:
#             missing_plz.append(origin)
#     try:
#         d_node_id = int(plz_ch[destination][1])
#     except:
#         d_node_id = None
#         if destination not in missing_plz:
#             missing_plz.append(destination)
#
#     print(rowname, end="\r")
#     return pd.Series([o_node_id, d_node_id])
#
#
# od_chsum_df[['o_node_id', 'd_node_id']] = od_chsum_df.apply(
#     lambda row: od_func_ch(row['fromPlz'], row['toPlz'], row.name), axis=1)
#
# df = pd.DataFrame(data={"missing_plz": missing_plz})
# df.to_csv("./missing_plz.csv", sep=',', index=False)
#
# od_chsum_df = pd.DataFrame.dropna(od_chsum_df)  # in case there are missing nuts not defined in the dictionary
# od_chsum_df.to_csv(str(out_path) + "/od_chsum_df.csv", sep=",", index=None)
#
# print(len(od_chsum_df))
# print(len(missing_plz))
# od_chsum_df.head()