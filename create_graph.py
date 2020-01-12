import pandas as pd
import pickle
import geopandas as gpd
import shapely.geometry as geo
import networkx as nx
import os

def sw_nodes(new_id, splitted_ways_dict, nodes_dict):
    coord_list = []
    nodes_list = splitted_ways_dict[new_id][2]
    for node in nodes_list:
        coord_list.append(nodes_dict[int(node)])
    line = geo.LineString(coord_list)
    return line

def create_graph_func(network_path):
    if os.path.isfile(str(network_path) + "/eu_network_largest_graph_bytime.gpickle") == False:
        print('Graph creation begins ...')
        #create a graph from network
        G = nx.Graph()
        G_isolated = nx.Graph()

        # import the network created from the OSM file
        train = pd.read_csv(str(network_path) + '\gdf_MTP_europe.csv')
        edges = train [["start_node_id", "end_node_id", "time(s)", "new_id"]]
        edges_list = edges.values.tolist()

        # introduce every way as edge with attributes of time and new_id
        for start, end, time, new_id in edges_list:
            G.add_edge(int(start), int(end), time = time, new_id = new_id)
            G_isolated.add_edge(int(start), int(end), time = time, new_id = new_id)
        start_Nn=G.number_of_nodes()
        start_Ne=G.number_of_edges()

        # export graph of original network to file (without excluding any edge or island)
        nx.write_gpickle(G, str(network_path) + "/eu_network_graph_bytime.gpickle")

        # Identify the largest component and the "isolated" nodes
        components = list(nx.connected_components(G)) # list because it returns a generator
        components.sort(key=len, reverse=True)
        longest_networks = []
        for i in range(0, len(components)):
            net = components[i]
            longest_networks.append(len(net))
            if i == 10:
                break

        largest = components.pop(0)
        isolated = set(g for cc in components for g in cc)
        num_isolated = G.order() - len(largest)

        # keep only the largest island of the original graph (so all nodes are reachable in the graph)
        # remove isolated nodes from G
        G.remove_nodes_from(isolated)
        end_Nn=G.number_of_nodes()
        end_Ne=G.number_of_edges()

        # export final graph only containing largest island from the network to file
        nx.write_gpickle(G, str(network_path) + "/eu_network_largest_graph_bytime.gpickle")

        print('Input edges: '+str(len(edges_list)))
        print('Start/End N_nodes: ' + str(start_Nn) + '/' + str(end_Nn))
        print('Start/End N_edges: ' + str(start_Ne) + '/' + str(end_Ne))

        print('N isolated nodes: ' +  str(num_isolated))
        print('10 largest networks (nodes): ' + str(longest_networks))
    else:
        G = nx.read_gpickle(str(network_path) + '/eu_network_largest_graph_bytime.gpickle')
        G_isolated = nx.read_gpickle(str(network_path) + '/eu_network_graph_bytime.gpickle')
        print('Graph with largest network already exists, graph loaded')

    # create shapefile with all nodes/edges excluded from the final graph (only for visual purpose)
    if os.path.isfile(str(network_path) + "/eu_isolated_graph_bytime.gpickle") == False:
        #IMPORT nodes_europe
        file = open(str(network_path) + "/europe_nodes_dict2056.pkl", 'rb')
        nodes_dict = pickle.load(file)

        file = open(str(network_path) + "/europe_ways_splitted_dict.pkl", 'rb')
        splitted_ways_dict = pickle.load(file)
        file.close()

        iso_edges = G_isolated.edges(list(isolated))
        iso_edges_df_1 = pd.DataFrame.from_records(list(iso_edges), columns = ["start_node_id","end_node_id"])
        iso_edges_df_2 = pd.DataFrame.from_records(list(iso_edges), columns = ["end_node_id","start_node_id"])
        train_iso = train[['new_id','start_node_id','end_node_id','nodes_list']]

        iso_edges_df = pd.concat ([iso_edges_df_1,iso_edges_df_2],sort=False).drop_duplicates().reset_index(drop=True)
        intersected_df = pd.merge(iso_edges_df,train_iso, how='inner')

        intersected_df['geometry'] = intersected_df.apply(lambda row: sw_nodes(row['new_id'],
                                                                               splitted_ways_dict,
                                                                               nodes_dict), axis=1)

        intersected_gdf = gpd.GeoDataFrame(intersected_df)
        intersected_gdf.crs = {"init": "EPSG:4326"}
        intersected_gdf = intersected_gdf.to_crs({"init": "EPSG:2056"})
        intersected_gdf.to_file(str(network_path) + "/isolated_graph.shp")

        # export GRAPH to file
        nx.write_gpickle(G_isolated, str(network_path) + "/eu_isolated_graph_bytime.gpickle")

        print('Isolated ways: ' + str(len(intersected_df)))
    else:
        print('Graph files and shp files of this network already exist in out_path')

# #Create the tree of nodes composing the node
# #do we supose that nearest neighbours can only be "start" or "end" points of each way? or nodes in between can be as well?

# #OPTION 3: input only nodes in largest network in G
# G_nodes = list(G.nodes)#.sort(key=float)
# G_nodes.sort(key=float)
# G_lonlat = []
# i = 0
# node_sel = G_nodes[i]
# for id in list(nodes_europe):
#     if node_sel == int(id):
#         lonlat = nodes_europe[id]
#         G_lonlat.append(lonlat)
#         if i < len(G_nodes)-1:
#             i+=1
#             node_sel = G_nodes[i]
# print(len(G_lonlat))

# #Find nearest neighbours for a given list of points created in previous cell for option 1 or 2
# closest_nodes =[]
# cn_coord = []
# # tree = spatial.KDTree(all_nodes)  #option 1
# # tree = spatial.KDTree(startend_nodes)  #option 2
# tree = spatial.KDTree(G_lonlat) #option 3
# # tree.data

# pts = list(city_lonlat.values()) #LIST COINTAINING POINTS TO FIND NN IN THE NETWORK
# nn = tree.query(pts)

# for i in nn[1]:
# #     coord = all_nodes[i]   #THIS WILL ONLY WORK WITH OPTION 1, AS THE COORDINATES ARE EXTRACTED FROM ALL_NODES
# #     coord = startend_nodes[i]   #THIS WILL ONLY WORK WITH OPTION 2, AS THE COORDINATES ARE EXTRACTED FROM ALL_NODES
#     coord = G_lonlat[i]      #THIS WILL ONLY WORK WITH OPTION 3, AS THE COORDINATES ARE EXTRACTED FROM ALL_NODES

#     closest_nodes.append(int(list(nodes_europe.keys())[list(nodes_europe.values()).index((coord[0],coord[1]))]))
#     cn_coord.append(coord)

# print(i)
# print(nn[1])
# print(closest_nodes)
# print(cn_coord)