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
                       border_file,
                       official_counting,
                       nuts_path,
                       mikrodaten,
                       out_path=None,
                       ):
    # networks = [
    #     # [raw_file, way_types, shp_file],
    #     ['alps-latest.osm.bz2', 1234567, 'bci_polygon30k_4326.shp'],
    #     ['europe-latest.osm.bz2', 123, None]
    #             ]

    network_list = []
    path_list = []
    for raw_file, way_types, shp_file in networks:
        if out_path is not None:
            export_files = True
            rawfile_name = ntpath.split(raw_file)[1].split('-')[0]
            network_path = str(out_path) + '/' + str(rawfile_name) + str(way_types)
            path_list.append(network_path)
        else:
            export_files = False

        network_objects = parse_network(raw_file=raw_file,
                                        out_path=out_path,
                                        highway_types=way_types,
                                        shp_file=shp_file)

        network_list.append(network_objects)


    if len(network_list) > 1:
        if export_files:
            network_path = str(out_path) + '/' + str(ntpath.split(path_list[0])[1]) + str(ntpath.split(path_list[1])[1])
            network_objects = merge_networks_funct(network_list,
                                                   original_path=path_list[0],
                                                   secondary_path=path_list[1],
                                                   out_path=network_path)
        else:
            network_objects = merge_networks_funct(network_objects)








    # if merged_network != None:
    #     network_path = merged_network

    find_bc(network_path=network_path,
            border_file=border_file,
            bc_path=r'C:/Users/Ion/IVT/OSM_python/freight_data/freight/official_counting_ot.csv')


    europe_data(network_path=network_path,
                nuts_path=nuts_path,
                europe_data_path=mikrodaten)


    rounting_funct(network_path=network_path,
                   border_file=border_file,
                   official_count_file=r'C:/Users/Ion/IVT/OSM_python/freight_data/freight/official_counting.csv',
                   training=True)
