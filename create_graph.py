import pandas as pd
import geopandas as gpd
import shapely.geometry as geo
import networkx as nx
import os
import datetime


def create_shp_largest(G, nodes_dict, splitted_ways_dict, gdf, out_path, filename, list_nodes=None):
    if list_nodes:
        g_edges = G.edges(list_nodes)
    else:
        g_edges = G.edges()

    g_edges_df1 = pd.DataFrame.from_records(list(g_edges), columns=["start_node_id", "end_node_id"])
    g_edges_df2 = pd.DataFrame.from_records(list(g_edges), columns=["end_node_id", "start_node_id"])
    all_edges = gdf[['new_id', 'start_node_id', 'end_node_id', 'nodes_list']]

    edges_df = pd.concat([g_edges_df1, g_edges_df2], sort=False).drop_duplicates().reset_index(drop=True)
    intersected_df = pd.merge(edges_df, all_edges, how='inner')

    intersected_df['geometry'] = intersected_df.apply(lambda row: sw_nodes(row['new_id'],
                                                                           splitted_ways_dict,
                                                                           nodes_dict), axis=1)
    intersected_gdf = gpd.GeoDataFrame(intersected_df)

    intersected_gdf.crs = "epsg:4326"
    intersected_gdf = intersected_gdf.to_crs("epsg:2056")

    intersected_gdf['geometry'].to_file(str(out_path) + "/" + str(filename) + ".shp", encoding='utf-8')

    print(datetime.datetime.now(), 'Shp file created successfully with ' + str(len(intersected_df)) + ' ways.')


def sw_nodes(new_id, splitted_ways_dict, nodes_dict):
    coord_list = []
    nodes_list = splitted_ways_dict[new_id][2]
    for node in nodes_list:
        coord_list.append(nodes_dict[int(node)])
    line = geo.LineString(coord_list)
    return line


def create_graph_func(out_path, gdf, nodes_dict, splitted_ways_dict):
    # if os.path.isfile(str(out_path) + "/eu_network_largest_graph_bytime.gpickle") is False or out_path is not None:
    print(datetime.datetime.now(), 'Graph creation begins ...')
    # create a graph from network
    G = nx.Graph()
    G_isolated = nx.Graph()

    # import the network created from the OSM file
    # train = pd.read_csv(str(out_path) + '\gdf_MTP_europe.csv')
    edges = gdf[["start_node_id", "end_node_id", "time(s)", "new_id", "way_type", "length(m)"]]
    edges_list = edges.values.tolist()

    # introduce every way as edge with attributes of time and new_id
    for start, end, time, new_id, way_type, length in edges_list:
        G.add_edge(int(start), int(end), length=length, time=time, new_id=new_id, way_type=way_type)
        G_isolated.add_edge(int(start), int(end), length=length, time=time, new_id=new_id, way_type=way_type)
    start_Nn = G.number_of_nodes()
    start_Ne = G.number_of_edges()

    # export graph of original network to file (without excluding any edge or island)
    if out_path is not None:
        nx.write_gpickle(G, str(out_path) + "/eu_network_graph_bytime.gpickle")

    # Identify the largest component and the "isolated" nodes
    components = list(nx.connected_components(G))  # list because it returns a generator
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
    end_Nn = G.number_of_nodes()
    end_Ne = G.number_of_edges()

    # export final graph only containing largest island from the network to file
    if out_path is not None:
        nx.write_gpickle(G, str(out_path) + "/eu_network_largest_graph_bytime.gpickle")

        # Create shp file with final graph
        print(datetime.datetime.now(), 'Creating shp file of largest network with epsg:2056 ...')
        create_shp_largest(G, nodes_dict, splitted_ways_dict, gdf, out_path, 'eu_network_largest_graph_bytime', list_nodes=None)

    print(datetime.datetime.now(), 'Input edges: ' + str(len(edges_list)))
    print(datetime.datetime.now(), 'Start/End N_nodes: ' + str(start_Nn) + '/' + str(end_Nn))
    print(datetime.datetime.now(), 'Start/End N_edges: ' + str(start_Ne) + '/' + str(end_Ne))

    print(datetime.datetime.now(), 'N isolated nodes: ' + str(num_isolated))
    print(datetime.datetime.now(), '10 largest networks (nodes): ' + str(longest_networks))
    print('------------------------------------------------------------------------')
    # else:
    #     G = nx.read_gpickle(str(out_path) + '/eu_network_largest_graph_bytime.gpickle')
    #     G_isolated = nx.read_gpickle(str(out_path) + '/eu_network_graph_bytime.gpickle')

    # Identify the largest component and the "isolated" nodes
    # components = list(nx.connected_components(G_isolated))  # list because it returns a generator
    # components.sort(key=len, reverse=True)
    # longest_networks = []
    # for i in range(0, len(components)):
    #     net = components[i]
    #     longest_networks.append(len(net))
    #     if i == 10:
    #         break
    # isolated = set(g for cc in components for g in cc)
    # print(datetime.datetime.now(), 'Graph with largest network already exists, graph loaded')
    # print('------------------------------------------------------------------------')

    return G

    # create shapefile with all nodes/edges excluded from the final graph (only for visual purpose)
    # if os.path.isfile(str(out_path) + "/eu_isolated_graph_bytime.gpickle") is False and \
    #         len(longest_networks) > 1:
    #     print(datetime.datetime.now(), 'Creating shpfile with graphs isolatated nodes ...')
    #
    #     # create shp file of isolated networks from original graph
    #     create_shp_largest(G, isolated, nodes_dict, splitted_ways_dict, gdf, out_path, 'isolated_graph')
    #
    #     # export GRAPH to file
    #     nx.write_gpickle(G_isolated, str(out_path) + "/eu_isolated_graph_bytime.gpickle")
    #
    #     # print(datetime.datetime.now(), 'Isolated ways: ' + str(len(intersected_df)))
    #     print('------------------------------------------------------------------------')
    # else:
    #     print(datetime.datetime.now(), 'Network does not have isolated nodes or shapefile of them already exists in out_path ')
    #     print('------------------------------------------------------------------------')

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