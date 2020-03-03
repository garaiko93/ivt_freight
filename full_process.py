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
        border_file = str(data_path) + '/bci_path.shp'
        official_count_file = str(data_path) + '/official_counting.csv'
        bc_path = str(data_path) + '/official_counting_ot.csv'
        nuts_path = str(data_path) + '/nuts_borders'
        mikrodaten = str(data_path) + '/GQGV_2014_Mikrodaten.csv'

    # -------------------------------------------------------------------------------------------------------------
    #  PARSE DEFINED NETWORKS
    # -------------------------------------------------------------------------------------------------------------
    networkObj_list = []
    path_list = []
    for raw_file, way_types, shp_file in networks:
        if out_path is not None:
            rawfile_name = ntpath.split(raw_file)[1].split('-')[0]
            if len(networks) > 1:
                network_path = str(out_path) + '/' + str(rawfile_name) + str(way_types)
                path_list.append(network_path)
            else:
                network_path = str(out_path)
        else:
            network_path = None

        network_objects = parse_network(raw_file=raw_file,
                                        out_path=network_path,
                                        highway_types=way_types,
                                        shp_file=shp_file)

        networkObj_list.append(network_objects)

    # -------------------------------------------------------------------------------------------------------------
    #  MERGE NETWORKS IF THERE ARE 2 DIFFERENT NETWORKS
    # -------------------------------------------------------------------------------------------------------------
    if len(networkObj_list) > 1:
        if out_path:
            out_path = str(ntpath.split(path_list[0])[0]) + '/' + str(ntpath.split(path_list[0])[1]) + str(ntpath.split(path_list[1])[1])
            print(out_path, path_list)
            original_path = path_list[0]
            secondary_path = path_list[1]
            networkObj_list = None
        else:
            original_path = None
            secondary_path = None

        network_objects = merge_networks_funct(network_objects=networkObj_list,
                                               original_path=original_path,
                                               secondary_path=secondary_path,
                                               out_path=out_path)
    else:
        network_objects = networkObj_list[0]

    # -------------------------------------------------------------------------------------------------------------
    #  FIND BORDER CROSSINGS
    # -------------------------------------------------------------------------------------------------------------
    network_objects = find_bc(network_objects=network_objects,
                    network_path=out_path,
                    border_file=border_file,
                    bc_path=bc_path)

    network_objects = europe_data(network_objects=network_objects,
                                  network_path=out_path,
                                  nuts_path=nuts_path,
                                  europe_data_path=mikrodaten)
    #
    #
    # rounting_funct(network_path=network_path,
    #                border_file=border_file,
    #                official_count_file=official_count_file,
    #                training=True)
