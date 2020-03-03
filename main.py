# import sys
# sys.path.insert(0, 'codes')
import datetime
import argparse

#  my functions
from network_parser import parse_network
from create_graph import create_graph_func
from merge_networks import merge_networks_funct
from bc_official import find_bc
from data_manipulating import europe_data
from routing import rounting_funct
from full_process import full_process_funct


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
#  RUN FULL PROCESS
# -------------------------------------------------------------------------------------------------------------
networks = [
        # [raw_file, way_types, shp_file],
    # [r'C:/Users/Ion/IVT/OSM_data/liechtenstein-latest.osm.bz2', 567, None],
    # [r'C:/Users/Ion/IVT/OSM_data/liechtenstein-latest.osm.bz2', 1234, None],
    # [r'C:/Users/Ion/IVT/OSM_data/liechtenstein-latest.osm.bz2', 567, None],
    ['/cluster/scratch/gaion/alps-latest.osm.bz2', 1234567, '/cluster/home/gaion/freight/data/bci_polygon30k_4326.shp'],
    # ['/cluster/scratch/gaion/europe-latest.osm.bz2', 123, None]
                ]
full_process_funct(networks,
                   # border_file,
                   # official_counting,
                   # nuts_path,
                   # mikrodaten,
                   # out_path=r'C:/Users/Ion/IVT/OSM_python/networks'
                   data_path='/cluster/home/gaion/freight/data',
                   out_path='/cluster/home/gaion/freight/networks/ch1234567')

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
#         1: 'motorway',
#         2: 'trunk',
#         3: 'primary',
#         4: 'secondary',
#         5: 'tertiary',
#         6: 'unclassified',
#         7: 'residential'

# parse_network(raw_file=d['raw_file'],
#               out_path=d['out_path'],
#               shp_file=d['shp_file'],
#               export_files=d['export_files'])
# parse_network(raw_file='/cluster/scratch/gaion/alps-latest.osm.bz2',
#               out_path='/cluster/home/gaion/freight/networks/ch1234567',
#               highway_types=1234567,
#               shp_file='/cluster/home/gaion/freight/data/bci_polygon30k_4326.shp')
# parse_network(raw_file='/cluster/scratch/gaion/europe-latest.osm.bz2',
#               out_path='/cluster/home/gaion/freight/networks/ch1234',
#               highway_types=1234,
#               shp_file='/cluster/home/gaion/freight/data/bci_polygon30k_4326.shp')
# parse_network(raw_file='/cluster/scratch/gaion/europe-latest.osm.bz2',
#               out_path='/cluster/home/gaion/freight/networks/ch567',
#               highway_types=567,
#               shp_file='/cluster/home/gaion/freight/data/bci_polygon30k_4326.shp')
# parse_network(raw_file='/cluster/scratch/gaion/europe-latest.osm.bz2',
#               out_path='/cluster/home/gaion/freight/networks/eu1234',
#               highway_types=123,
#               shp_file=None,
#               export_files=True)
# parse_network(raw_file=r'C:/Users/Ion/IVT/OSM_data/switzerland-latest.osm.bz2',
#               out_path=r'C:/Users/Ion/IVT/OSM_python/networks/eu1234',
#               highway_types=123,
#               shp_file=None,
#               export_files=True)
# parse_network(raw_file=r'C:/Users/Ion/IVT/OSM_data/alps-latest.osm.bz2',
#               out_path=r'C:/Users/Ion/IVT/OSM_python/networks/ch1234567',
#               highway_types=1234567,
#               shp_file=r'C:/Users/Ion/IVT/OSM_python/switzerland/ch_bordercrossings/swiss_border/bci_polygon30k_4326.shp',)
# parse_network(raw_file=r'C:/Users/Ion/IVT/OSM_data/switzerland-latest.osm.bz2',
#               out_path=r'C:/Users/Ion/IVT/OSM_python/test/ch1/network_files',
#               highway_types=1,
#               shp_file=None,
#               export_files=True)
# parse_network(raw_file=r'C:/Users/Ion/IVT/OSM_data/switzerland-latest.osm.bz2',
#               out_path=r'C:/Users/Ion/IVT/OSM_python/test/ch2/network_files',
#               highway_types=2,
#               shp_file=None,
#               export_files=True)

# -------------------------------------------------------------------------------------------------------------
# MERGE NETWORKS
# -------------------------------------------------------------------------------------------------------------
# merge_networks_funct(original_path,
#                      secondary_path,
#                      out_path)
# merge_networks_funct(original_path='/cluster/home/gaion/freight/networks/ch4/network_files',
#                      secondary_path='/cluster/home/gaion/freight/networks/eu123/network_files',
#                      out_path='/cluster/home/gaion/freight/networks/eu123ch4/network_files')

# merge_networks_funct(original_path=r'C:/Users/Ion/IVT/OSM_python/test/lie123',
#                      secondary_path=r'C:/Users/Ion/IVT/OSM_python/test/lie4',
#                      out_path=r'C:/Users/Ion/IVT/OSM_python/test/lie1234')

# -------------------------------------------------------------------------------------------------------------
# FIND SWISS BORDER CROSSINGS WITH THE NETWORK
# -------------------------------------------------------------------------------------------------------------
# find_bc(network_path, border_file, bc_data)
# find_bc(network_path='/cluster/home/gaion/freight/networks/eu123ch4/network_files',
#         border_file='/cluster/home/gaion/freight/data/bci_path.shp',
#         bc_path='/cluster/home/gaion/freight/data/official_counting_ot.csv')
# find_bc(network_path=r'C:/Users/Ion/IVT/OSM_python/networks/ch1234567',
#         border_file=r'C:/Users/Ion/IVT/OSM_python/switzerland/ch_bordercrossings/swiss_border/bci_path.shp',
#         bc_path=r'C:/Users/Ion/IVT/OSM_python/freight_data/freight/official_counting_ot.csv')

# -------------------------------------------------------------------------------------------------------------
# MANIPULATE FREIGHT DATA FOR ROUTING
# -------------------------------------------------------------------------------------------------------------
# europe_data(out_path, graph_path, nuts_path, europe_data_path)
# europe_data(out_path='/cluster/home/gaion/freight/networks/eu123ch4/freight_data',
#             graph_path='/cluster/home/gaion/freight/networks/eu123ch4/bc_official',
#             nuts_path='/cluster/home/gaion/freight/data/nuts_borders',
#             europe_data_path='/cluster/home/gaion/freight/data/GQGV_2014_Mikrodaten.csv')
# europe_data(out_path=r'C:/Users/Ion/IVT/OSM_python/test/eu123ch4/freight_data',
#             graph_path=r'C:/Users/Ion/IVT/OSM_python/test/eu123ch4/bc_official',
#             nuts_path=r"C:/Users/Ion/IVT/OSM_python/freight_data/nuts_borders/nuts_borders",
#             europe_data_path=r"C:/Users/Ion/IVT/OSM_python/freight_data/freight/gqgv/GQGV_2014/GQGV_2014_Mikrodaten.csv")
# europe_data(network_path=r'C:/Users/Ion/IVT/OSM_python/test/lie1234',
#             nuts_path=r"C:/Users/Ion/IVT/OSM_python/freight_data/nuts_borders/nuts_borders",
#             europe_data_path=r"C:/Users/Ion/IVT/OSM_python/freight_data/freight/gqgv/GQGV_2014/GQGV_2014_Mikrodaten.csv")

# -------------------------------------------------------------------------------------------------------------
# ROUTING
# -------------------------------------------------------------------------------------------------------------
# def rounting_funct(network_path, border_file, official_count_file, training=False):

# rounting('/cluster/home/gaion/freight/networks/ch123/network_files',
#         '/cluster/home/gaion/freight/data/bci_path.shp',
#         '/cluster/home/gaion/freight/networks/chlie123/network_files')
# rounting_funct(network_path=r'C:/Users/Ion/IVT/OSM_python/test/lie1234',
#                border_file=r'C:/Users/Ion/IVT/OSM_python/switzerland/ch_bordercrossings/swiss_border/bci_path.shp',
#                official_count_file=r'C:/Users/Ion/IVT/OSM_python/freight_data/freight/official_counting.csv',
#                training=True)

# -------------------------------------------------------------------------------------------------------------
# ANALYSE RESULTS OF ROUTING
# -------------------------------------------------------------------------------------------------------------
#


print(datetime.datetime.now(), 'Main script ends.')
