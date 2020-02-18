# import sys
# sys.path.insert(0, 'codes')
import datetime
from network_parser import parse_network
from create_graph import create_graph_func
from merge_networks import merge_networks_funct
from bc_official import find_bc
import argparse

print(datetime.datetime.now(), 'Main script begins')

# Define arguments parser
# parser = argparse.ArgumentParser(description='Cut and analyse a graph for a certain input area.')
# # network_parser
# parser.add_argument('raw_file', help='path to original OSM file')
# parser.add_argument('out_path', help='path where to export files')
# parser.add_argument('--shp_file', dest="shp_file", help='path to shp file')
# parser.add_argument('--export_files', dest="export_files", help="True/False")
# # create_graph
# args = parser.parse_args()
# d = vars(args)
# print(args, args.shp_file, args.export_files)
# print(d['raw_file'], d['out_path'])

# -------------------------------------------------------------------------------------------------------------
#  PARSE NETWORK FROM OSM FILE AND CREATE DATABASE
# -------------------------------------------------------------------------------------------------------------
# '''
# This function creates from an OSM file a network database composed of nodes and ways, through this steps:
# 1. The raw file is splitted into _nodes, _ways and _relations files for easier later processing:
# 2. Create dictionaries from nodes and ways containing respective information (key = node/way id):
#         -nodes: nodes_dict[id] = (x_coord, y_coord) [in both, epsg:2056 and epsg:4326]
#         -ways:  ways_dict[id] = [way_type, way_maxspeed, waynodes_list, way_maxspeed_f, way_maxspeed_b,
#                                         speed_val, oneway, lanes, lanes_f, lanes_b]
# 3. From the ways database, new files of splitted_ways (ways which only share start or end nodes, no in between nodes)
#   are created.
# 4. Create a shape file in epsg:2056 from the splitted ways files.

# This python script requires some arguments:
# 1. --raw_file: Directory to OSM raw file of format .osm.bz2 (i.e.: 'C:/Users/.../liechtenstein-latest.osm.bz2').
# 2. --out_path: Directory of folder where new files will be created
# (i.e.: 'C:/Users/.../test').
# 3. --shp_file (optional): Directory of shp file which defines area of new scenario
# (i.e.: "C:/Users/.../bci_polygon30k_4326.shp'")
# '''

# parse_network(raw_file=d['raw_file'],
#               out_path=d['out_path'],
#               shp_file=d['shp_file'],
#               export_files=d['export_files'])
parse_network(raw_file='C:/Users/Ion/IVT/OSM_data/liechtenstein-latest.osm.bz2',
              out_path='C:/Users/Ion/IVT/OSM_python/test/lie/MTP',
              shp_file=None,
              export_files=True)
              # shp_file='C:/Users/Ion/IVT/OSM_python/switzerland/ch_bordercrossings/swiss_border/bci_polygon30k_4326.shp'

# -------------------------------------------------------------------------------------------------------------
# MERGE NETWORKS
# -------------------------------------------------------------------------------------------------------------
# merge_networks_funct(original_path, secondary_path, out_path)
merge_networks_funct('/cluster/home/gaion/freight/networks/ch123/network_files',
                     '/cluster/home/gaion/freight/networks/lie123/network_files',
                     '/cluster/home/gaion/freight/networks/chlie123/network_files')

# -------------------------------------------------------------------------------------------------------------
# FIND SWISS BORDER CROSSINGS WITH THE NETWORK
# -------------------------------------------------------------------------------------------------------------
# find_bc(network_path, border_file, bc_data)
find_bc('/cluster/home/gaion/freight/networks/ch123/network_files',
        '/cluster/home/gaion/freight/data/bci_path.shp',
        '/cluster/home/gaion/freight/networks/chlie123/network_files')

# -------------------------------------------------------------------------------------------------------------
# MANIPULATE FREIGHT DATA FOR ROUTING
# -------------------------------------------------------------------------------------------------------------
# europe_data(out_path, graph_path, nuts_path, europe_data_path)
europe_data('/cluster/home/gaion/freight/networks/ch123/freight_data',
            '/cluster/home/gaion/freight/data/bci_path.shp',
            '/cluster/home/gaion/freight/networks/chlie123/network_files')

# -------------------------------------------------------------------------------------------------------------
# ROUTING
# -------------------------------------------------------------------------------------------------------------
# find_bc(network_path, border_file, bc_data)
find_bc('/cluster/home/gaion/freight/networks/ch123/network_files',
        '/cluster/home/gaion/freight/data/bci_path.shp',
        '/cluster/home/gaion/freight/networks/chlie123/network_files')


print(datetime.datetime.now(), 'Main script ends')
