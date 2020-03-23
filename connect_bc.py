import pandas as pd
import pickle
import geopandas as gpd
import numpy as np
from shapely.geometry import Point, LinearRing, Polygon
import networkx as nx
import datetime
from scipy import spatial
import copy

from data_manipulating import nuts_merging

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
    return G_lonlat, tree


def route_bc(node, g_123, g_1234567, nodes_europe, tree, G_lonlat, centroid=None):
    # This finds the closest node in network ch1234567 of the centroid to start the routing
    if centroid:
        g_in_lonlat, g_in_tree = closest_node(g_1234567, nodes_europe)
        nn = g_in_tree.query(centroid)
        coord = g_in_lonlat[nn[1]]
        node = int(
            list(nodes_europe.keys())[list(nodes_europe.values()).index((coord[0], coord[1]))])

    # this gives the closest nodes id (of ch 123 to route with) from the given node id, this can be: border crossing or nuts centroids closest node id
    nn = tree.query(node)
    coord = G_lonlat[nn[1]]
    closest_node_id = int(
        list(nodes_europe.keys())[list(nodes_europe.values()).index((coord[0], coord[1]))])

    # then rout both nodes
    path = nx.astar_path(g_1234567, node, closest_node_id, weight='time')
    for i in range(0, len(path) - 1):
        node1 = path[i]
        node2 = path[i + 1]
        new_id = g_1234567[node1][node2]['new_id']
        time = g_1234567[node1][node2]['time']
        way_type = g_1234567[node1][node2]['way_type']

        # and save the ways, implement them into ch_123
        g_123.add_edge(node1, node2, time=time, new_id=new_id, way_type=way_type)
    return g_123


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


def connect_bc_funct(border_file, cut_nonelected=True, network_objects=None, data_path=None, out_path=None):
    # import crossing_onlypoints with crossing points of ch1234567 containing coordinates
    # import ch123 and ch1234567
    # for each border crossing, if it is not in a 123 way, find closest point to 123 on both sides of border
    # route between the border crossing and closest points in both sides of the border
    # keep the ways used in the routing
    # create a final graph with ch123 and the connected border crossings

    print(datetime.datetime.now(), 'Starting process of connecting border points and swiss nuts with unclassified ways.')
    print('------------------------------------------------------------------------')
    out_path = r'C:/Users/Ion/IVT/OSM_python/networks/lie123'
    data_path = r'C:/Users/Ion/IVT/OSM_data'
    network_objects = None

    print(datetime.datetime.now(), 'Loading files.')
    if network_objects:
        g_ch1234567 = network_objects[0]
        crossing_onlypoints = network_objects[5]
        nodes_europe = network_objects[3]
    else:
        g_ch1234567 = nx.read_gpickle(str(out_path) + '/network_files/eu_network_graph_bytime.gpickle')
        crossing_onlypoints = gpd.read_file(str(out_path) + '/bc_official/crossing_onlypoints.shp')
        file = open(str(out_path) + "/network_files/europe_nodes_dict4326.pkl", 'rb')
        nodes_europe = pickle.load(file)
        file.close()

    ch_border = gpd.read_file(border_file)  # border has to be in 4326, loaded one is in 2056
    ch_border.crs = "epsg:2056"
    ch_border = ch_border.to_crs("epsg:4326")
    nuts_path = str(data_path) + '/nuts_borders'
    print(datetime.datetime.now(), 'Files loaded.')
    print('------------------------------------------------------------------------')

    g_ch123 = copy.deepcopy(g_ch1234567)
    print(datetime.datetime.now(), 'Nodes/ways in g_ch1234567: ' + str(len(g_ch123.nodes)) + '/' + str(len(g_ch123.edges)))

    for (u, v, c) in g_ch1234567.edges.data('way_type'):
        for way_type in ['secondary', 'tertiary', 'residential', 'unclassified']:
            if way_type in c:
                g_ch123.remove_edge(u, v)
    g_ch123.remove_nodes_from(list(nx.isolates(g_ch123)))
    print(datetime.datetime.now(), 'Nodes/ways in g_ch123: ' + str(len(g_ch123.nodes)) + '/ ' + str(len(g_ch123.edges)))
    print('------------------------------------------------------------------------')

    # This splits both network graphs between in and out of the swiss border
    g_ch123_in, g_ch123_out = split_graphs(g_ch123, ch_border, nodes_europe)
    g_ch1234567_in, g_ch1234567_out = split_graphs(g_ch1234567, ch_border, nodes_europe)
    print('------------------------------------------------------------------------')

    # This creates the tree of the ch123 graphs to find the closest nodes
    g_in_lonlat, g_in_tree = closest_node(g_ch123_in, nodes_europe)
    g_out_lonlat, g_out_tree = closest_node(g_ch123_out, nodes_europe)
    print('------------------------------------------------------------------------')

    # HASTA AQUI FUNCIONA, AHORA FALTA AÑADIR START_NODE_ID Y ENDD_NODE_ID A crossing_onlypoints EN BC_OFFICIAL.PY
    # -----------------------------------------------------------------------------
    # CONNECT BORDER CROSSINGS WITH CH123
    # -----------------------------------------------------------------------------
    print(datetime.datetime.now(),
          'Number of edges in graphs (in/out graphs) BEFORE connecting border crossings: ' + str(len(g_ch123_in.edges)) + '/' + str(len(g_ch123_out.edges)))

    for index, row in crossing_onlypoints.iterrows():
        way_id = row['new_id']
        start_node_id = row['start_node']
        end_node_id = row['end_node_i']
        print(start_node_id,end_node_id)
        # Check if bc is from a principal highway
        for (u, v, c) in g_ch123.edges.data('new_id'):
            if way_id == c:
                continue

        in_ch = ch_border.contains(Point(nodes_europe[start_node_id]))
        if in_ch[0] == True:
            in_node = start_node_id
            out_node = end_node_id
        else:
            in_node = end_node_id
            out_node = start_node_id

        # route in and out point in respective graphs to closest point in ch123
        # it may be that it is not connected, then 'continue'
        g_ch123_in = route_bc(in_node, g_ch123_in, g_ch1234567_in, nodes_europe, g_in_tree, g_in_lonlat)
        g_ch123_out = route_bc(out_node, g_ch123_out, g_ch1234567_out, nodes_europe, g_out_tree, g_out_lonlat)

    print(datetime.datetime.now(),
          'Number of edges in graphs (in/out graphs) AFTER connecting border crossings: ' + str(
              len(g_ch123_in.edges)) + '/' + str(len(g_ch123_out.edges)))
    print('------------------------------------------------------------------------')

    # -----------------------------------------------------------------------------
    # CONNECT NUTS CENTROIDS WITH CH123
    # -----------------------------------------------------------------------------
    unique_nuts_gdf = nuts_merging(nuts_path)
    for index, row in unique_nuts_gdf.iterrows():
        nutid = row['NUTS_ID']
        if 'CH' in nutid:
            nut_poly = row['geometry']
            centroid = nut_poly.centroid

            g_ch123_in = route_bc(None, g_ch123_in, g_ch1234567_in, nodes_europe, g_in_tree, g_in_lonlat, centroid)

    print(datetime.datetime.now(),
          'Number of edges in graphs (in/out graphs) AFTER connecting border crossings: ' + str(
              len(g_ch123_in.edges)) + '/' + str(len(g_ch123_out.edges)))
    print('------------------------------------------------------------------------')

    # at the end, after merging both out and in graphs the edges which cross the border will have to be added again,
    # as this process does not count with them
    g_ch123_connected = nx.compose(g_ch123_in, g_ch123_out)
    for index, row in crossing_onlypoints.iterrows():
        start_node_id = row['start_node_id']
        end_node_id = row['end_node_id']
        new_id = row['new_id']
        time = float(row['time'])
        way_type = row['way_type']

        # and save the ways, implement them into ch_123
        g_ch123_connected.add_edge(start_node_id, end_node_id, time=time, new_id=new_id, way_type=way_type)

    print(datetime.datetime.now(), 'Nodes/ways in final graph g_ch123_connected: ' + str(len(g_ch123_connected.nodes)) + ', ' + str(len(g_ch123_connected.edges)))


    # export graph and shp file of final network
    if out_path:
        nx.write_gpickle(g_ch123_connected, str(out_path) + "/bc_official/ch_connected_graph_bytime.gpickle")

    # In case the none elected border crossings want to be deleted, activate this
    if cut_nonelected:
        g_ch123_connected = cut_nonelected_bc(g_ch123_connected, network_objects=network_objects, out_path=out_path)

    # return g_ch123_connected
