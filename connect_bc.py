import pandas as pd
import pickle
import geopandas as gpd
import numpy as np
from shapely.geometry import Point, LinearRing, Polygon
import networkx as nx
import datetime
from scipy import spatial
import os
import random
import copy

from data_manipulating import nuts_merging
from create_graph import create_shp_largest

def print_islands(G, graph_name):
    # Identify the largest component and the "isolated" nodes
    components = list(nx.connected_components(G))  # list because it returns a generator
    components.sort(key=len, reverse=True)
    longest_networks = []
    for i in range(0, len(components)):
        net = components[i]
        longest_networks.append(len(net))
        if i == 10:
            break

    print(datetime.datetime.now(), str(graph_name) + ': 10 largest networks (nodes): ' + str(longest_networks))
    print('------------------------------------------------------------------------')


def cut_nonelected_bc(G, network_objects=None, out_path=None):
    # -----------------------------------------------------------------------------
    # MODIFY GRAPH, REMOVE EDGES THAT ARE NOT IN ELECTED CROSSING POINTS
    # -----------------------------------------------------------------------------
    # IMPORT G original graph
    # (not the one with the largest island as this procedure may change this largest island so the largest network will be taken later from this output)
    if out_path is not None:
        # # IMPORT europe_ways_splitted_dict
        file = open(str(out_path) + "/network_files/europe_ways_splitted_dict.pkl", 'rb')
        europe_ways_splitted_dict = pickle.load(file)
        file.close()
        bc_df = gpd.read_file(str(out_path) + '/bc_official/bc_df.shp')
        crossing_onlypoints = gpd.read_file(str(out_path) + '/bc_official/crossing_onlypoints.shp')
    else:
        europe_ways_splitted_dict = network_objects[3]
        crossing_onlypoints = network_objects[5]
        bc_df = network_objects[6]

    print(datetime.datetime.now(), 'G graph (Nnodes/Nedges): ' + str(G.number_of_nodes()) + '/' + str(G.number_of_edges()))
    print(datetime.datetime.now(), 'Nways in europe_ways_splitted_dict: ' + str(len(europe_ways_splitted_dict)))

    # DELETE ways that contain border crossings not considered for the routing
    all_newids = list(crossing_onlypoints.new_id)  # all crossings found in the network
    elected_newids = list(bc_df.new_id)
    # the ones that have passed all the filters and match the official border crossings from the freight data
    none_elected = [newid for newid in all_newids if newid not in elected_newids]
    print(datetime.datetime.now(), 'All new_ids: ' + str(len(all_newids)))
    print(datetime.datetime.now(), 'Elected new_ids: ' + str(len(elected_newids)))
    print(datetime.datetime.now(), 'Non elected new_ids: ' + str(len(none_elected)))

    # remove none_elected edges from graph and export
    deleted_edges = 0
    for id in none_elected:
        o_node = europe_ways_splitted_dict[id][2][0]
        d_node = europe_ways_splitted_dict[id][2][-1]
        if (G.has_edge(int(o_node), int(d_node))) == True:
            G.remove_edge(int(o_node), int(d_node))
            deleted_edges += 1

    # Identify the largest component and the "isolated" nodes
    components = list(nx.connected_components(G))  # list because it returns a generator
    components.sort(key=len, reverse=True)
    longest_networks = []
    for i in range(0, 1):
        net = components[i]
        longest_networks.append(len(net))
    largest = components.pop(0)
    isolated = set(g for cc in components for g in cc)
    num_isolated = G.order() - len(largest)

    # remove isolated nodes from G
    G.remove_nodes_from(isolated)

    if out_path is not None:
        nx.write_gpickle(G, str(out_path) + "/eu_network_graph_with_official_bc.gpickle")

    print(datetime.datetime.now(), 'G graph (Nnodes/Nedges): ' + str(G.number_of_nodes()) + '/' + str(G.number_of_edges()))
    print(datetime.datetime.now(), 'New graph has: ' + str(
        len([len(c) for c in sorted(nx.connected_components(G), key=len, reverse=True)])) + ' island with ' + str(
        G.number_of_nodes()) + ' nodes')
    print(datetime.datetime.now(), 'From ' + str(len(none_elected)) + ' none_elected Edges: ' + str(
        deleted_edges) + ' deleted correctly (others not in graph)')
    print('------------------------------------------------------------------------')
    print(datetime.datetime.now(), 'New graph exported as "eu_network_graph_with_official_bc"')
    print('------------------------------------------------------------------------')
    return G


# Split both graphs by the swiss border
def split_graphs(g, ch_border, nodes_europe):
    print(datetime.datetime.now(), 'Splitting graph into in and out of swiss border ...')
    print(datetime.datetime.now(), 'Number of nodes/edges in original graph: ' + str(len(g.nodes)) + '/ ' + str(len(g.edges)))

    g_in = copy.deepcopy(g)
    g_out = copy.deepcopy(g)
    for node in g.nodes():
        in_ch = ch_border.contains(Point(nodes_europe[node]))
        if in_ch[0] == True:
            g_out.remove_node(node)
        else:
            g_in.remove_node(node)

    print(datetime.datetime.now(), 'Number of nodes/edges in IN graph: ' + str(len(g_in.nodes)) + '/ ' + str(len(g_in.edges)))
    print(datetime.datetime.now(), 'Number of nodes/edges in OUT graph: ' + str(len(g_out.nodes)) + '/ ' + str(len(g_out.edges)))
    print('------------------------------------------------------------------------')
    return g_in, g_out


# Build tree for KDTree nearest neighbours search, in G only start and end nodes are included
# OPTION 3: input only nodes in largest network in G
def closest_node(G, nodes_europe):
    G_nodes = list(G.nodes)
    G_nodes.sort(key=float)
    G_lonlat = []
    i = 0
    node_sel = G_nodes[i]
    for id in list(nodes_europe):
        if node_sel == int(id):
            lonlat = nodes_europe[id]
            G_lonlat.append(lonlat)
            if i < len(G_nodes) - 1:
                i += 1
                node_sel = G_nodes[i]
    tree = spatial.KDTree(G_lonlat)
    print(datetime.datetime.now(), 'KDTree has: ' + str(len(G_lonlat)) + ' nodes.')
    return tree, G_lonlat


def route_bc(node, closest_node_id, g_123, g_1234567, nodes_europe, new_nodes, new_ways, tree, G_lonlat, found_count, centroid=None, g_in_tree=None, g_in_lonlat=None):
    # node = in_node
    # g_123=g_ch123_in
    # g_1234567=g_ch1234567_in
    # nodes_europe=nodes_europe
    # tree=g_in_tree
    # G_lonlat=g_in_lonlat
    # This finds the closest node in network ch1234567 of the centroid to start the routing
    if node is None:
        nn = g_in_tree.query(centroid)
        coord = g_in_lonlat[nn[1]]
        node = int(list(nodes_europe.keys())[list(nodes_europe.values()).index((coord[0], coord[1]))])
        # print('node found', node)

    # this gives the closest nodes id (of ch 123 to route with) from the given node id, this can be: border crossing or nuts centroids closest node id
    if closest_node_id is None:
        nn = tree.query(nodes_europe[node])
        coord = G_lonlat[nn[1]]
        closest_node_id = int(list(nodes_europe.keys())[list(nodes_europe.values()).index((coord[0], coord[1]))])

    # then rout both nodes
    try:
        path = nx.astar_path(g_1234567, node, closest_node_id, weight='time')
        for i in range(0, len(path) - 1):
            node1 = path[i]
            node2 = path[i + 1]
            new_id = g_1234567[node1][node2]['new_id']
            # length = g_1234567[node1][node2]['length']
            time = g_1234567[node1][node2]['time']
            way_type = g_1234567[node1][node2]['way_type']

            # and save the ways, implement them into ch_123
            # g_123.add_edge(node1, node2, time=time, length=length, new_id=new_id, way_type=way_type)
            g_123.add_edge(node1, node2, time=time, new_id=new_id, way_type=way_type)
            new_nodes[node1] = nodes_europe[node1]
            new_nodes[node2] = nodes_europe[node2]
            new_ways[new_id] = [node1, node2, [node1, node2]]

        found_count[0] += 1
        # print('path found')
    except:
        # print('path not found', node, closest_node_id)
        found_count[1] += 1
        pass
    return g_123, new_nodes, new_ways, found_count


def connect_bc_funct(cut_nonelected=False, network_objects=None, data_path=None, out_path=None, border_file4326=None):
    # import crossing_onlypoints with crossing points of ch1234567 containing coordinates
    # import ch123 and ch1234567
    # for each border crossing, if it is not in a 123 way, find closest point to 123 on both sides of border
    # route between the border crossing and closest points in both sides of the border
    # keep the ways used in the routing
    # create a final graph with ch123 and the connected border crossings

    # out_path = r'C:/Users/Ion/IVT/OSM_python/networks/'
    # data_path = r'C:/Users/Ion/IVT/OSM_data'
    # border_file = str(data_path) + '/borderOSM_polygon_2056.shp'
    # network_objects = None
    # g_ch123_connected = nx.read_gpickle(str(ch1234567_path) + '/network_files/ch_connected_graph_bytime.gpickle')


    # if out_path is None:
    #     g_ch1234567 = network_objects[0]
    #     gdf = network_objects[1]
    #     splitted_ways_dict = network_objects[2]
    #     nodes_europe = network_objects[3]
    #     ch_border = network_objects[4]  # border has to be in 4326, loaded one is in 2056
    #     crossing_onlypoints = network_objects[5]
    # else:
    ch1234567_path = str(out_path) + '/ch1234567'
    ch123_path = str(out_path) + '/ch123'
    eu123_path = str(out_path) + '/eu123'
    eu123ch_path = str(out_path) + '/eu123ch4567'
    if os.path.isfile(str(ch1234567_path) + "/network_files/ch_connected_graph_bytime.gpickle") is False or out_path is None:
        print(datetime.datetime.now(), 'Starting process of connecting border points and swiss nuts with unclassified ways.')
        print('------------------------------------------------------------------------')

        print(datetime.datetime.now(), 'Loading files.')
        # files from ch1234567 and eu123
        #     g_ch1234567 = nx.read_gpickle(str(ch1234567_path) + '/network_files/eu_network_graph_bytime.gpickle')
        # i think this should also be without islands, to avoid finding a closest node
        g_ch123 = nx.read_gpickle(str(ch123_path) + '/network_files/eu_network_largest_graph_bytime.gpickle')
        g_ch1234567 = nx.read_gpickle(str(ch1234567_path) + '/network_files/eu_network_largest_graph_bytime.gpickle')
        # here is preferable to avoid islands as they may be far from switzerland
        g_eu123 = nx.read_gpickle(str(eu123_path) + '/network_files/eu_network_largest_graph_bytime.gpickle')
        crossing_onlypoints = gpd.read_file(str(ch1234567_path) + '/bc_official/crossing_onlypoints.shp')

        file = open(str(ch1234567_path) + "/network_files/europe_nodes_dict4326.pkl", 'rb')
        nodes_europe = pickle.load(file)
        file.close()
        border_file4326
        # border_file = str(data_path) + '/Switzerland_OSM_polygon_4326.shp'
        ch_border = gpd.read_file(border_file4326)  # border has to be in 4326, loaded one is in 2056

        new_nodes = {}
        new_ways = {}
        # ch_border.crs = "epsg:2056"
        # ch_border = ch_border.to_crs("epsg:4326")
        nuts_path = str(data_path) + '/nuts_borders'

        print(datetime.datetime.now(), 'Files loaded.')

        print_islands(g_ch123, 'g_ch123')
        print_islands(g_ch1234567, 'g_ch1234567')
        print_islands(g_eu123, 'g_eu123')
        print('------------------------------------------------------------------------')

        # Creates graph of ch123 from graph ch1234567 and split into IN and OUT graphs
        # g_ch123 = copy.deepcopy(g_ch1234567)
        print(datetime.datetime.now(), 'Nodes/ways in g_ch1234567: ' + str(len(g_ch1234567.nodes)) + '/' + str(len(g_ch1234567.edges)))

        # for (u, v, c) in g_ch1234567.edges.data('way_type'):
        #     for way_type in ['secondary', 'tertiary', 'residential', 'unclassified']:
        #         if way_type in c:
        #             g_ch123.remove_edge(u, v)
        # g_ch123.remove_nodes_from(list(nx.isolates(g_ch123)))


        print(datetime.datetime.now(), 'Nodes/ways in g_ch123: ' + str(len(g_ch123.nodes)) + '/ ' + str(len(g_ch123.edges)))
        print('------------------------------------------------------------------------')

    if os.path.isfile(str(ch1234567_path) + '/network_files/g_ch1234567_out.gpickle') is False or out_path is None:
        # This splits both network graphs between in and out of the swiss border
        g_ch123_in, g_ch123_out = split_graphs(g_ch123, ch_border, nodes_europe)
        g_ch1234567_in, g_ch1234567_out = split_graphs(g_ch1234567, ch_border, nodes_europe)

        nx.write_gpickle(g_ch123_in, str(ch1234567_path) + '/network_files/g_ch123_in.gpickle')
        nx.write_gpickle(g_ch123_out, str(ch1234567_path) + '/network_files/g_ch123_out.gpickle')
        nx.write_gpickle(g_ch1234567_in, str(ch1234567_path) + '/network_files/g_ch1234567_in.gpickle')
        nx.write_gpickle(g_ch1234567_out, str(ch1234567_path) + '/network_files/g_ch1234567_out.gpickle')
    elif os.path.isfile(str(ch1234567_path) + "/network_files/ch_connected_graph_bytime.gpickle") is False or out_path is None:
        g_ch123_in = nx.read_gpickle(str(ch1234567_path) + '/network_files/g_ch123_in.gpickle')
        g_ch123_out = nx.read_gpickle(str(ch1234567_path) + '/network_files/g_ch123_out.gpickle')
        g_ch1234567_in = nx.read_gpickle(str(ch1234567_path) + '/network_files/g_ch1234567_in.gpickle')
        g_ch1234567_out = nx.read_gpickle(str(ch1234567_path) + '/network_files/g_ch1234567_out.gpickle')

        print_islands(g_ch123_in, 'g_ch123_in')
        print_islands(g_ch123_out, 'g_ch123_out')
        print_islands(g_ch1234567_in, 'g_ch1234567_in')
        print_islands(g_ch1234567_out, 'g_ch1234567_out')

    if os.path.isfile(str(ch1234567_path) + "/network_files/ch_connected_graph_bytime.gpickle") is False or out_path is None:
        # This creates the tree of the ch123 graphs to find the closest nodes
        g_in_tree, g_in_lonlat = closest_node(g_ch123_in, nodes_europe)
        g_out_tree, g_out_lonlat = closest_node(g_ch123_out, nodes_europe)
        # g_infull_lonlat, g_infull_tree = closest_node(g_ch1234567_in, nodes_europe)
        g_full_tree, g_full_lonlat = closest_node(g_ch1234567, nodes_europe)
        print('------------------------------------------------------------------------')

    # -----------------------------------------------------------------------------
    # CONNECT BORDER CROSSINGS WITH CH123
    # -----------------------------------------------------------------------------
    if os.path.isfile(str(ch1234567_path) + "/network_files/ch_connected_graph_bytime.gpickle") is False or out_path is None:
        print(datetime.datetime.now(),
              'Number of edges in graphs (in/out graphs) BEFORE connecting border crossings: ' + str(len(g_ch123_in.edges)) + '/' + str(len(g_ch123_out.edges)))
        found_count = [0, 0]
        for index, row in crossing_onlypoints.iterrows():
            way_id = row['new_id']
            start_node_id = row['start_node']
            end_node_id = row['end_node_i']
            # Check if bc is from a principal highway
            # for (u, v, c) in g_ch123.edges.data('new_id'):
            #     if way_id == c:
            #         continue

            in_ch = ch_border.contains(Point(nodes_europe[start_node_id]))
            if in_ch[0] == True:
                in_node = start_node_id
                out_node = end_node_id
            else:
                in_node = end_node_id
                out_node = start_node_id

            # route in and out point in respective graphs to closest point in ch123
            # it may be that it is not connected, then 'continue'
            g_ch123_in, new_nodes, new_ways, found_count = route_bc(in_node, None, g_ch123_in, g_ch1234567_in, nodes_europe, new_nodes, new_ways, g_in_tree, g_in_lonlat, found_count)
            g_ch123_out, new_nodes, new_ways, found_count = route_bc(out_node, None, g_ch123_out, g_ch1234567_out, nodes_europe, new_nodes, new_ways, g_out_tree, g_out_lonlat, found_count)

        print(datetime.datetime.now(), 'Found path: ' + str(found_count[0]))
        print(datetime.datetime.now(), 'Not found path: ' + str(found_count[1]))
        print(datetime.datetime.now(),
              'Number of edges in graphs (in/out graphs) AFTER connecting border crossings: ' + str(
                  len(g_ch123_in.edges)) + '/' + str(len(g_ch123_out.edges)))

        print_islands(g_ch123_in, 'g_ch123_in')
        print_islands(g_ch123_out, 'g_ch123_out')
        print('------------------------------------------------------------------------')

        # -----------------------------------------------------------------------------
        # CONNECT NUTS CENTROIDS WITH CH123
        # -----------------------------------------------------------------------------
        unique_nuts_gdf = nuts_merging(nuts_path)
        found_count = [0, 0]
        for index, row in unique_nuts_gdf.iterrows():
            nutid = row['NUTS_ID']
            if 'CH' in nutid:
                nut_poly = row['geometry']
                centroid = nut_poly.centroid
                centroid_coords = (centroid.x, centroid.y)

                g_ch123_in, new_nodes, new_ways, found_count = route_bc(None, None, g_ch123_in, g_ch1234567_in, nodes_europe, new_nodes, new_ways, g_in_tree, g_in_lonlat, found_count, centroid_coords, g_full_tree, g_full_lonlat)


        print(datetime.datetime.now(), 'Found path: ' + str(found_count[0]))
        print(datetime.datetime.now(), 'Not found path: ' + str(found_count[1]))
        print(datetime.datetime.now(),
              'Number of edges in graphs (in/out graphs) AFTER connecting nuts centroids: ' + str(
                  len(g_ch123_in.edges)) + '/' + str(len(g_ch123_out.edges)))
        print_islands(g_ch123_in, 'g_ch123_in')
        print('------------------------------------------------------------------------')

        # -----------------------------------------------------------------------------
        # ADD EDGES WHICH CROSS BORDER (DELETED WHEN SPLITTING GRAPH TO IN/OUT)
        # -----------------------------------------------------------------------------
        # at the end, after merging both out and in graphs the edges which cross the border will have to be added again,
        # as this process does not count with them
        g_ch123_connected = nx.compose(g_ch123_in, g_ch123_out)
        print(datetime.datetime.now(),
              'Number of edges in connected graph after merging IN and OUT graphs WITHOUT border crossings: ' + str(
                  len(g_ch123_connected.edges)))
        print_islands(g_ch123_connected, 'g_ch123_connected')

        for index, row in crossing_onlypoints.iterrows():
            try:
                start_node_id = row['start_node']
                end_node_id = row['end_node_i']
                new_id = row['new_id']
                # length = row['length']
                time = g_ch1234567[start_node_id][end_node_id]['time']
                way_type = row['way_type']

                # and save the ways, implement them into ch_123
                # g_ch123_connected.add_edge(start_node_id, end_node_id, time=time, length=length, new_id=new_id, way_type=way_type)
                g_ch123_connected.add_edge(start_node_id, end_node_id, time=time, new_id=new_id, way_type=way_type)
            except:
                continue


        print(datetime.datetime.now(),
              'Number of edges in connected graph after merging IN and OUT graphs WITH border crossings: ' + str(
                  len(g_ch123_connected.edges)))
        print_islands(g_ch123_connected, 'g_ch123_connected')

        # -----------------------------------------------------------------------------
        # CONNECT REMAINING ISLANDS OF FINAL CONNECTED GRAPH
        # -----------------------------------------------------------------------------
        # Last, as there are some islands in the last connected graph due to splitting of graph, connected islands:
        g_ch123_connected_largest = copy.deepcopy(g_ch123_connected)
        print_islands(g_ch123_connected_largest, 'g_ch123_connected_largest')

        components = list(nx.connected_components(g_ch123_connected_largest))  # list because it returns a generator
        components.sort(key=len, reverse=True)
        largest = components.pop(0)
        isolated = set(g for cc in components for g in cc)
        g_ch123_connected_largest.remove_nodes_from(isolated)

        print_islands(g_ch123_connected_largest, 'g_ch123_connected_largest')

        g_tree_large, g_lonlat_large = closest_node(g_ch123_connected_largest, nodes_europe)
        found_count = [0, 0]
        for i in range(len(components)):
            net = components[i]
            node = random.choice(list(net))
            g_ch123_connected, new_nodes, new_ways, found_count = route_bc(node, None, g_ch123_connected, g_ch1234567, nodes_europe, new_nodes, new_ways, g_tree_large, g_lonlat_large, found_count)

        print(datetime.datetime.now(),
              'Number of edges in connected SWISS graph after connecting islands in connected graph: ' + str(
                  len(g_ch123_connected.edges)))

        print_islands(g_ch123_connected, 'g_ch123_connected')

        # In case the none elected border crossings want to be deleted, activate this
        if cut_nonelected:
            g_ch123_connected = cut_nonelected_bc(g_ch123_connected, network_objects=network_objects, out_path=out_path)

        # Join final connected graph with eu123 graph, to complete the full network
        g_eu123_connected = nx.compose(g_ch123_connected, g_eu123)
        print(datetime.datetime.now(),
              'Number of edges in connected EUROPE graph: ' + str(
                  len(g_eu123_connected.edges)))

        print_islands(g_eu123_connected, 'g_eu123_connected')

        # -----------------------------------------------------------------------------
        # ADD TO EUROPE NETWORK FILES THE ADDED NODES AND WAYS TO THE CONNECTED GRAPH
        # -----------------------------------------------------------------------------
        # file = open(str(eu123_path) + "/network_files/europe_nodes_dict4326.pkl", 'rb')
        # all_europe_nodes = pickle.load(file)
        # file.close()
        # europe_nodes_merged = {**all_europe_nodes, **new_nodes}
        # # with open(str(out_path) + '/europe_nodes_dict4326.pkl', 'wb') as f:
        # #     pickle.dump(europe_nodes_merged, f, pickle.HIGHEST_PROTOCOL)
        # print(len(all_europe_nodes), len(all_europe_nodes))
        #
        # file = open(str(eu123_path) + "/network_files/europe_ways_splitted_dict.pkl", 'rb')
        # all_europe_sw = pickle.load(file)
        # file.close()
        # europe_sw_merged = {**all_europe_sw, **new_ways}
        #
        #
        # eu_gdf = pd.read_csv(str(eu123_path) + "/network_files/gdf_MTP_europe.csv", low_memory=False)
        #
        # new_ways_df = pd.DataFrame.from_dict(new_ways, orient='index', columns=['start_node_id', 'end_node_id', 'nodes_list'])
        # new_ways_df = new_ways_df.reset_index()
        # new_ways_df = new_ways_df.rename(columns={"index": "new_id"})
        #
        # eu_gdf = pd.concat([new_ways_df, eu_gdf])

        # export graph and shp file of final network
        if os.path.isfile(str(ch1234567_path) + "/network_files/ch_connected_graph_bytime.gpickle") is False and out_path:
            nx.write_gpickle(g_eu123_connected, str(eu123_path) + "/network_files/eu_connected_graph_bytime.gpickle")
            nx.write_gpickle(g_ch123_connected, str(ch1234567_path) + "/network_files/ch_connected_graph_bytime.gpickle")
            if os.path.isfile(str(out_path) + "/network_files/eu_connected_graph_bytime.shp") is False:
                create_shp_largest(g_ch123_connected, None, None, None,
                                   str(ch1234567_path) + "/network_files", 'ch_connected_graph_bytime', list_nodes=None)
                # create_shp_largest(g_eu123_connected, europe_nodes_merged, europe_sw_merged, eu_gdf,
                #                    str(eu123_path) + "/network_files", 'eu_connected_graph_bytime', list_nodes=None)
                create_shp_largest(g_eu123_connected, None, None, None,
                                   str(eu123ch_path) + "/network_files", 'eu_connected_graph_bytime', list_nodes=None)
    else:
        print(datetime.datetime.now(), 'Connected files already exist.')
        print('------------------------------------------------------------------------')
        print('------------------------------------------------------------------------')
    # else:
    #     print(datetime.datetime.now(), 'Connected network graph was found.')
    #     if os.path.isfile(str(ch1234567_path) + "/network_files/ch_connected_graph_bytime.shp") is False:
    #         g_ch123_connected = nx.read_gpickle(str(ch1234567_path) + '/network_files/ch_connected_graph_bytime.gpickle')
    #         create_shp_largest(g_ch123_connected, nodes_europe, splitted_ways_dict, gdf, str(out_path) + "/network_files", 'ch_connected_graph_bytime', list_nodes=None)


    # FINALLY THIS GRAPH SHOULD BE MERGED WITH EU123




    # if out_path is None:
    #     network_objects = [g_ch123_connected,
    #                        network_objects[1],
    #                        network_objects[2],
    #                        network_objects[3],
    #                        network_objects[4],
    #                        network_objetcs[5],
    #                        network_objetcs[6],
    #                        network_objetcs[7]
    #                        ]
    #     return network_objects
