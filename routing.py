import pandas as pd
import pickle
import geopandas as gpd
import networkx as nx
from shapely.geometry import Point
import os
import datetime


def rounting_funct(network_path, border_file, official_count_file, training=False):
    print(datetime.datetime.now(), 'Routing process begins ...')

    out_path = str(network_path) + '/routing_results'
    # out_path = str(network_path) + '/freight_data'

    if not os.path.exists(str(out_path)):
        os.makedirs(str(out_path))
        print(datetime.datetime.now(), 'Directory created.')
    else:
        print(datetime.datetime.now(), 'Directory exists.')

    # Find best graph:
    if os.path.isfile(str(network_path) + "/network_files/eu_connected_graph_bytime.gpickle") is True:
        graph_path = str(network_path) + '/network_files/eu_connected_graph_bytime.gpickle'
        print(datetime.datetime.now(), 'Graph loaded: eu_connected_graph_bytime')
    elif os.path.isfile(str(network_path) + "/bc_official/eu_network_graph_with_official_bc.gpickle") is True:
        graph_path = str(network_path) + '/bc_official/eu_network_graph_with_official_bc.gpickle'
        print(datetime.datetime.now(), 'Graph loaded: eu_network_graph_with_official_bc')
    else:
        graph_path = str(network_path) + '/network_files/eu_network_largest_graph_bytime.gpickle'
        print(datetime.datetime.now(), 'Graph loaded: eu_network_largest_graph_bytime')

    # Load graph from selected network to route
    G = nx.read_gpickle(graph_path)
    print(datetime.datetime.now(), 'Graph has: ' + str(
        len([len(c) for c in sorted(nx.connected_components(G), key=len, reverse=True)])) + ' island with '
          + str(G.number_of_nodes()) + '/' + str(G.number_of_edges()) + ' (Nnodes/Nedges)')

    # Load nodes dictionary from selected network
    file = open(str(network_path) + "/network_files/europe_nodes_dict2056.pkl", 'rb')
    nodes_europe_2056 = pickle.load(file)
    file.close()
    print(datetime.datetime.now(), 'Nnodes in nodes_europe_2056: ' + str(len(nodes_europe_2056)))

    # Load wayidbycp dictionary
    file = open(str(network_path) + "/bc_official/wayidbycp_dict.pkl", 'rb')
    wayidbycp = pickle.load(file)
    file.close()
    print(datetime.datetime.now(), 'Nways in wayidbycp: ' + str(len(wayidbycp)))

    # LOAD MANIPULATED FREIGHT DATA (EU)
    # od_europesum_df = pd.read_csv(str(network_path) + "/freight_data/od_europesum_df.csv")
    od_europesum_df = pd.read_csv(str(network_path) + "/freight_data/od_europesum_df_test.csv")
    print(datetime.datetime.now(), 'Nroutes in od_europesum_df: ' + str(len(od_europesum_df)))

    # This code does the routing between the o_node_id and the d_node_id in both data frames (od_europesum_df and od_chsum_df)
    # checking every new_id from the ways that conform the shortest path in the networks graph (fastest in this case, with time attribute)
    # to see if any of them is stored as a 'border crossing'
    # if it is: it stores in thee 'border_crossings' list which is the correspondent name of the crossing and the 'new_ids' in the dataframe
    def fastest_path(start_point, end_point, rowname):
        border_crossing = []
        new_ids = []
        if start_point != end_point:
            path = nx.astar_path(G, start_point, end_point, weight='time')
            for i in range(0,len(path)-1):
                node1 = path[i]
                node2 = path[i+1]
                new_id = G[node1][node2]['new_id']
                if new_id in list(wayidbycp):
                    border_crossing.append(wayidbycp[new_id])
                    new_ids.append(new_id)
        # print(datetime.datetime.now(), 'Iteration process: ' + str(rowname), end="\r")
        return pd.Series([border_crossing, new_ids], index=['border_crossing', 'new_id'])

    if os.path.isfile(str(out_path) + '/od_europesum_df1.csv') == False:
        od_europesum_df[['border_crossings','new_ids']] = od_europesum_df.apply(lambda row: fastest_path(
            row['o_node_id'],
            row['d_node_id'],
            row.name), axis=1)

        # DROP NONE ('[]') ROWS FROM DATAFRAMES
        od_europesum_empty_df = pd.DataFrame().reindex_like(od_europesum_df).dropna()
        droprows = []
        for i in range(len(od_europesum_df)):
            if len(od_europesum_df.iloc[i]['border_crossings']) == 0:
                droprows.append(i)
        od_europesum_empty_df = od_europesum_empty_df.append(od_europesum_df.loc[droprows])
        od_europesum_df = od_europesum_df.drop(od_europesum_df.index[droprows])

        # EXPORT DATAFRAMES CHECKPOINT before proceding to next step
        if training == False:
            od_europesum_df.to_csv(str(out_path) + "/od_europesum_df1.csv", sep=",", index=None, encoding='latin1')
    else:
        od_europesum_df = pd.read_csv(str(out_path) + "/od_europesum_df1.csv", encoding='latin1')
        print(datetime.datetime.now(), 'Routing was done, dataframe loaded od_europesum_df: ' + str(len(od_europesum_df)))


    # Depending where is the origin and destination nodes
    # this code identifies the IN and OUT border crossings for every situation (1, or more than 1)
    # this is stored in the 'in' and 'out' columns of each dataframe

    # load swiss border shapefile
    ch_border = gpd.read_file(border_file)['geometry'][0]
    none_bc = 0

    def in_out_bc(start_point, end_point, border_crossing, rowname, none_bc):
        from_ch = ch_border.contains(Point(nodes_europe_2056[int(start_point)]))
        to_ch = ch_border.contains(Point(nodes_europe_2056[int(end_point)]))
        if len(border_crossing) == 1:
            if from_ch == True and to_ch == False:
                out_bc = border_crossing[0]
                in_bc = None
            elif to_ch == True and from_ch == False:
                in_bc = border_crossing[0]
                out_bc = None
            else:
                in_bc = None
                out_bc = None
        elif len(border_crossing) > 1:
            if to_ch == False and from_ch == False:
                in_bc = border_crossing[0]
                out_bc = border_crossing[-1]
            elif to_ch == True and from_ch == True:
                out_bc = border_crossing[0]
                in_bc = border_crossing[-1]
            else:
                in_bc = None
                out_bc = None
        else:
            in_bc = None
            out_bc = None
            none_bc += 1
        # print(datetime.datetime.now(), 'Iteration process: ' + str(rowname), end="\r")
        return pd.Series([in_bc, out_bc], index=['in', 'out'])

    if os.path.isfile(str(out_path) + '/od_europesum_df1.csv') == False:
        od_europesum_df[['in', 'out']] = od_europesum_df.apply(lambda row: in_out_bc(
            int(row['o_node_id']),
            int(row['d_node_id']),
            row['border_crossings'],
            row.name,
            none_bc
        ), axis=1)

        if training == False:
            od_europesum_df.to_csv(str(out_path) + "/od_europesum_df2.csv", sep=",", index=None, encoding='latin1')
    else:
        od_europesum_df = pd.read_csv(str(out_path) + "/od_europesum_df2.csv", encoding='latin1')
        print(datetime.datetime.now(), 'In & Out already found, dataframe loaded od_europesum_df: ' + str(len(od_europesum_df)))


    # ACUMULATE COUNTING OF FREIGHT PER CROSSING POINT
    # Finally, this code, accumulates the freight counting of every freight trip of the dataframe depending: ZV,QV, TV (in,out) and BV(in,out)
    # it is accumulated for the border crossings found in previouse steps
    # exported in files 'eu_rounting.' or 'ch_rounting.' respectively

    # create dictinoary with Nr and border crossing point names
    official_df = pd.read_csv(official_count_file)

    bc_id = {}
    for i in range(len(official_df)):
        nr = official_df.iloc[i]['Nr.']
        name = official_df.iloc[i]['Name']
        bc_id[str(nr)] = name
    bc_id[' '] = 0
    bc_id[float('nan')] = 0
    bc_id['3070'] = 0  # not defined border crossing (the only one)

    bc_id_invert = dict(map(reversed, bc_id.items()))

    # FIRST, EUROPE DATA
    eu_routing_dict = {}

    # for i in list(wayidbycp):
    for i in list(bc_id):
        # if bc_id[i] == 0:
        #     continue
        #     crosspoint = wayidbycp[i]
        crosspoint = bc_id[i]
        eu_routing_dict[crosspoint] = [0, 0, 0, 0, 0, 0, 0, 0]

    for i in range(0, len(od_europesum_df)):
        weight_factor = od_europesum_df.iloc[i]['WEIGHTING_FACTOR']
        divisor = od_europesum_df.iloc[i]['DIVISOR']
        trip_freq = weight_factor / divisor

        #     in_bc = od_europesum_df.iloc[i]['in']
        #     out_bc = od_europesum_df.iloc[i]['out']
        in_bc = bc_id[od_europesum_df.iloc[i]['BORDER_CROSSING_IN']]
        out_bc = bc_id[od_europesum_df.iloc[i]['BORDER_CROSSING_OUT']]
        if in_bc == 0:
            in_bc = None
        if out_bc == 0:
            out_bc = None

        start_point = od_europesum_df.iloc[i]['o_node_id']
        end_point = od_europesum_df.iloc[i]['d_node_id']
        from_ch = ch_border.contains(Point(nodes_europe_2056[int(start_point)]))
        to_ch = ch_border.contains(Point(nodes_europe_2056[int(end_point)]))
        # in/out/through freight
        if in_bc is not None and out_bc is not None:
            if from_ch == False and to_ch == False:
                eu_routing_dict[in_bc][2] += trip_freq
                eu_routing_dict[out_bc][2] += trip_freq
                eu_routing_dict[in_bc][3] += trip_freq
                eu_routing_dict[out_bc][4] += trip_freq
            elif from_ch == True and to_ch == True:
                eu_routing_dict[in_bc][5] += trip_freq
                eu_routing_dict[out_bc][5] += trip_freq
                eu_routing_dict[in_bc][6] += trip_freq
                eu_routing_dict[out_bc][7] += trip_freq
        elif in_bc is not None and out_bc is None:
            eu_routing_dict[in_bc][0] += trip_freq
        elif out_bc is not None and in_bc is None:
            eu_routing_dict[out_bc][1] += trip_freq
        print(i, end="\r")

    # create dataframes and export to csv files
    eu_routing_df = pd.DataFrame(columns=['Nr', 'Name', 'ZV', 'QV', 'TV', 'in_TV', 'out_TV', 'BV', 'in_BV', 'out_BV'])
    j = 0
    for i in list(eu_routing_dict):
        eu_routing_df.loc[j] = [bc_id_invert[i],
                                i,
                                eu_routing_dict[i][0],
                                eu_routing_dict[i][1],
                                eu_routing_dict[i][2],
                                eu_routing_dict[i][3],
                                eu_routing_dict[i][4],
                                eu_routing_dict[i][5],
                                eu_routing_dict[i][6],
                                eu_routing_dict[i][7]]
        j += 1

    if training == False:
        #     eu_routing_df.to_csv(str(out_dir) + "\eu_routing_df.csv", sep = ",", index = None, encoding='latin1')
        eu_routing_df.to_csv(str(out_path) + "/real_routing_df.csv", sep=",", index=None, encoding='latin1')
        # EXPORT dictionaries TO FILE
        #     with open(str(out_dir) + '\eu_routing_dict' + '.pkl', 'wb') as f:
        with open(str(out_path) + '/real_routing_dict' + '.pkl', 'wb') as f:
            pickle.dump(eu_routing_dict, f, pickle.HIGHEST_PROTOCOL)
