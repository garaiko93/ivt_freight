# import sys
# sys.path.insert(0, 'codes')
import datetime
import os
import datetime
import ntpath
import argparse

#  my functions
from network_parser import parse_network
from merge_networks import merge_networks_funct
from bc_official import find_bc
from data_manipulating import europe_data
from routing import rounting_funct
from connect_bc import connect_bc_funct

def full_process_funct(networks,
                       border_file=r'C:/Users/Ion/IVT/OSM_python/switzerland/ch_bordercrossings/swiss_border/bci_path.shp',
                       official_count_file=r'C:/Users/Ion/IVT/OSM_python/freight_data/freight/official_counting.csv',
                       bc_path=r'C:/Users/Ion/IVT/OSM_python/freight_data/freight/official_counting_ot.csv',
                       nuts_path=r"C:/Users/Ion/IVT/OSM_python/freight_data/nuts_borders/nuts_borders",
                       mikrodaten=r"C:/Users/Ion/IVT/OSM_python/freight_data/freight/gqgv/GQGV_2014/GQGV_2014_Mikrodaten.csv",
                       out_path=None,
                       data_path=None
                       ):

    if data_path is not None:
        # data_path = 'C:/Users/Ion/IVT/OSM_data'
        # border_file = str(data_path) + '/bci_path.shp'
        border_file = str(data_path) + '/borderOSM_polygon_2056.shp'
        official_count_file = str(data_path) + '/official_counting.csv'
        bc_path = str(data_path) + '/official_counting_ot.csv'
        nuts_path = str(data_path) + '/nuts_borders'
        mikrodaten = str(data_path) + '/GQGV_2014_Mikrodaten.csv'

    # -------------------------------------------------------------------------------------------------------------
    #  PARSE DEFINED NETWORKS
    # -------------------------------------------------------------------------------------------------------------
    networkObj_list = []
    path_list = []
    for raw_file, way_types, shp_file, foldername in networks:
        if out_path is not None:
            network_path = str(out_path) + '/' + str(foldername)
            path_list.append(network_path)
        else:
            network_path = None

        # Call parsing function
        network_objects = parse_network(raw_file=raw_file,
                                        out_path=network_path,
                                        highway_types=way_types,
                                        shp_file=shp_file,
                                        data_path=data_path)
        networkObj_list.append(network_objects)

        '''
            OUTPUT: network_objects = [graph,                 1     (largest networks graph)
                                       gdf,                   2     (in epsg:2056)
                                       nodes_europe,          3     (in epsg:4326)
                                       splitted_ways_dict]    4     splitted_ways_dict[new_wayid] = [way_type, way_maxspeed, copy_nodes, way_maxspeed_f, way_maxspeed_b, speed_val, oneway, lanes, lanes_f, lanes_b]
        '''

    # -------------------------------------------------------------------------------------------------------------
    #  FIND BORDER CROSSINGS
    # -------------------------------------------------------------------------------------------------------------

        network_objects = find_bc(network_objects=network_objects,
                                  network_path=network_path,
                                  border_file=border_file,
                                  bc_path=bc_path)
        '''
        OUTPUT: network_objects = [graph,                 1
                                   gdf,                   2
                                   nodes_europe,          3
                                   splitted_ways_dict,    4
                                   bc_list,               5
                                   bc_df,                 6
                                   wayid_by_cp]           7
        '''
        connect_bc_funct(border_file=border_file,
                         cut_nonelected=False,
                         network_objects=network_objects,
                         data_path=data_path,
                         out_path=network_path)

    # -------------------------------------------------------------------------------------------------------------
    #  MERGE NETWORKS IF THERE ARE 2 DIFFERENT NETWORKS
    # -------------------------------------------------------------------------------------------------------------
    # to add in the future: attribute merge_network = [folder_name, networks_to_merge**]
    # if len(networkObj_list) > 1:
    #     if out_path:
    #         out_path = str(ntpath.split(path_list[0])[0]) + '/' + str(ntpath.split(path_list[0])[1]) + str(ntpath.split(path_list[1])[1])
    #
    #         original_path = path_list[0]
    #         secondary_path = path_list[1]
    #         networkObj_list = None
    #     else:
    #         original_path = None
    #         secondary_path = None
    #
    #     network_objects = merge_networks_funct(network_objects=networkObj_list,
    #                                            original_path=original_path,
    #                                            secondary_path=secondary_path,
    #                                            out_path=out_path)
    # else:
    #     if out_path is None:
    #         network_objects = networkObj_list[0]
    #     else:
    #         network_objects = None



    # network_objects = [G, network_objects[1], network_objects[2], network_objects[3], wayid_by_cp]
    # network_objects = europe_data(network_objects=network_objects,
    #                               network_path=out_path,
    #                               nuts_path=nuts_path,
    #                               europe_data_path=mikrodaten)
    #
    #
    # rounting_funct(network_path=network_path,
    #                border_file=border_file,
    #                official_count_file=official_count_file,
    #                training=True)
    #