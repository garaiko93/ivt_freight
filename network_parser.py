import gzip
import bz2file
import re
import pandas as pd
from tqdm import tqdm_notebook
import numpy as np
import pickle
import geopandas as gpd
import shapely.geometry as geo
from collections import defaultdict
import smtplib
from email import message
import time
import datetime
import csv
import os
from sklearn.neighbors import KDTree
from shapely.geometry import shape
from functools import partial
import pyproj
from shapely.ops import transform
from shapely.geometry import Point, Polygon,LineString,LinearRing, MultiPoint


def ms_func(tag_value, ms_strings):
    # this function filters/cleans/converts the content of the maximum speed tag in the 'ways' elements
    speed = ''.join(x for x in tag_value if x.isdigit())
    if speed != '':
        if 'mph' in tag_value:
            copy_speed = '{0:.7g}'.format(float(speed) * 1.609344)  # convert to kph
        else:
            copy_speed = float(speed)
    else:
        copy_speed = None

    if tag_value not in ms_strings:
        ms_strings.append(tag_value)
    return copy_speed

def split_ways(init_pos, nd_pos, way_id, count_id, copy_nodes):
    new_wayid = '_'.join([str(way_id), str(count_id)])

    start_node_id = copy_nodes[0]
    # start_latlon = nodes_europe[start_node_id]
    # start_x = start_latlon[0]
    # start_y = start_latlon[1]

    end_node_id = copy_nodes[len(copy_nodes) - 1]
    # end_latlon = nodes_europe[end_node_id]
    # end_x = end_latlon[0]
    # end_y = end_latlon[1]
    # data format is the same as in ways_dict, only changes the way_id and the list of nodes composing the way (if it is crossed by another way, if not everything stays the same)
    way_type = ways_dict[way_id][0]
    way_maxspeed = ways_dict[way_id][1]
    way_maxspeed_f = ways_dict[way_id][3]
    way_maxspeed_b = ways_dict[way_id][4]
    speed_val = ways_dict[way_id][5]
    oneway = ways_dict[way_id][6]
    lanes = ways_dict[way_id][7]
    lanes_f = ways_dict[way_id][8]
    lanes_b = ways_dict[way_id][9]

    splitted_ways_data = [way_type, way_maxspeed, copy_nodes, way_maxspeed_f, way_maxspeed_b, speed_val, oneway, lanes,
                          lanes_f, lanes_b]
    splitted_ways_dict[new_wayid] = splitted_ways_data

    splitted_ways.append((
        new_wayid, int(way_id), float(count_id), int(start_node_id), int(end_node_id), str(way_type),
        way_maxspeed, copy_nodes, way_maxspeed_f, way_maxspeed_b, speed_val, oneway, lanes, lanes_f, lanes_b
    ))

def parse_network(raw_file, shp_path, out_path):
    raw_file = 'C:/Users/Ion/IVT/OSM_data'
    shp_path = 'C:/Users/Ion/IVT/OSM_python/switzerland/ch_bordercrossings/swiss_border'
    out_path = 'C:/Users/Ion/IVT/OSM_python/test'
    osm_file = 'liechtenstein'

    if not os.path.exists(str(out_path)):
        os.makedirs(str(out_path))
        print('Directory created')
    else:
        print('Directory exists')
    print('------------------------------------------------------------------------')

    # -----------------------------------------------------------------------------
    # SPLIT OSM FILE IN FILES FOR: NODES, WAYS AND RELATIONS
    # -----------------------------------------------------------------------------
    node_check = 0
    way_check = 0
    relation_check = 0
    lines_nodes = 0
    lines_ways = 0
    lines_relations = 0
    lines_europe = 0

    # by reading the selected file line by line, for each xml element type this code splits the 3 of them in 3 different files for: NODES, WAYS and RELATIONS elements
    if os.path.isfile(str(out_path) + "\europe-latest_nodes.osm.bz2") == False and \
            os.path.isfile(str(out_path) + "\europe-latest_ways.osm.bz2") == False and \
            os.path.isfile(str(out_path) + "\europe-latest_relations.osm.bz2") == False:
        with bz2file.open(str(raw_file) + "/" + str(osm_file) + "-latest.osm.bz2") as f:
            with bz2file.open(str(out_path) + "\europe-latest_nodes.osm.bz2", 'wb') as f1:
                with bz2file.open(str(out_path) + "\europe-latest_ways.osm.bz2", 'wb') as f2:
                    with bz2file.open(str(out_path) + "\europe-latest_relations.osm.bz2", 'wb') as f3:
                        for line in tqdm_notebook(f):
                            lines_europe += 1
                            if b"<node" in line:
                                f1.write(line)
                                lines_nodes += 1

                            if b"<way" in line:
                                way_check = 1
                            if way_check == 1:
                                f2.write(line)
                                lines_ways += 1
                                if b"</way>" in line:
                                    way_check = 0

                            if b"<relation" in line:
                                relation_check = 1
                            if relation_check == 1:
                                f3.write(line)
                                lines_relations += 1
                                if b"</relation>" in line:
                                    relation_check = 0
    # -----------------------------------------------------------------------------
    # WAYS
    # -----------------------------------------------------------------------------
    if os.path.isfile(str(out_path) + "/europe_ways_dict.pkl") == False:
        ways = []
        way_check = 0
        ways_count = 0
        waynodes_list = []
        tag_list = []
        nodes_of_ways = []
        ms_strings = []
        ways_dict = {}
        way_maxspeed_f = None
        way_maxspeed_b = None
        way_maxspeed = None
        speed_val = None
        lanes_f = None
        lanes_b = None
        lanes = None
        nonspeed_ways = 0

        # this part of the code reads the WAYS file previously created line by line, taking the information that is relevant from its attributes and children elements
        with bz2file.open(str(out_path) + "\europe-latest_ways.osm.bz2") as f:
            for line in f:
                if b"<way" in line:
                    # records the way id
                    way_check = 1
                    ways_count += 1
                    m = re.search(rb'id="([0-9.]+)"', line)
                    way_id = m.group(1).decode('utf-8')
                if way_check == 1:
                    if b"<nd" in line:
                        # records each node id composing the way (these are reflected as <nd ref='...'> childrent elements into the <way> element)
                        m = re.search(rb'ref="([0-9.]+)"', line)
                        waynode = m.group(1).decode('utf-8')
                        waynodes_list.append(waynode)
                    if b"<tag" in line:
                        # saves the information contained by the chosen tags inside the way elements
                        m = re.search(rb'k="(.+)" v="(.+)"', line)
                        if m is not None:
                            tag_name = m.group(1).decode('utf-8')
                            tag_value = m.group(2).decode('utf-8')
                            tag_list.append(tag_name)

                            if tag_name == 'highway':
                                way_type = str(tag_value)
                            elif tag_name == 'maxspeed':
                                way_maxspeed, ms_strings = ms_func(tag_value, ms_strings)
                                speed_val = tag_value
                            elif tag_name == 'maxspeed:forward':
                                way_maxspeed_f, ms_strings = ms_func(tag_value, ms_strings)
                            elif tag_name == 'maxspeed:backward':
                                way_maxspeed_b, ms_strings = ms_func(tag_value, ms_strings)
                            elif tag_name == 'lanes':
                                lanes = tag_value
                            elif tag_name == 'lanes:forward':
                                lanes_f = tag_value
                            elif tag_name == 'lanes:backward':
                                lanes_b = tag_value
                            elif tag_name == 'oneway':
                                if tag_value == 'yes':
                                    oneway = 1
                                elif tag_value == 'no':
                                    oneway = 0
                    if b"</way>" in line:
                        # by changing the way_type on this condition it can be chosen the ways that will be saved
                        # as output from the original OSM file
                        if ('motorway' in way_type) or ('trunk' in way_type) or ('primary' in way_type):
                        # if 'secondary' in way_type:
                        # SET VALUE OF MAXSPEED by different tries, if nothing found, set by default
                            if way_maxspeed is None:
                                ms = [way_maxspeed_f, way_maxspeed_b]
                                ms = [x for x in ms if x is not None]
                                try:
                                    way_maxspeed = max(ms)
                                except:
                                    # SET DEFAULT VALUES
                                    nonspeed_ways += 1
                                    if way_type == 'motorway':
                                        way_maxspeed = 120
                                    elif way_type == 'motorway_link':
                                        way_maxspeed = 80
                                    elif way_type == 'trunk':
                                        way_maxspeed = 80
                                    elif way_type == 'trunk_link':
                                        way_maxspeed = 50
                                    elif way_type == 'primary':
                                        way_maxspeed = 80
                                    elif way_type == 'primary_link':
                                        way_maxspeed = 60
                                    elif way_type == 'secondary':
                                        way_maxspeed = 30
                                    elif way_type == 'secondary_link':
                                        way_maxspeed = 30
                                    elif way_type == 'tertiary':
                                        way_maxspeed = 25
                                    elif way_type == 'tertiary_link':
                                        way_maxspeed = 25

                            #                     if lanes is None:
                            #                         L = [lanes_b,lanes_f]
                            #                         L = [x for x in L if x is not None]
                            #                         try:
                            #                             lanes = sum(L)
                            #                         except:
                            #                             lanes = None

                            start_node_id = waynodes_list[0]  # first node of the list
                            end_node_id = waynodes_list[len(waynodes_list) - 1]  # last node of the list

                            # data of each way stored, saved in dictionary being the key the way_id
                            way_data = [way_type, way_maxspeed, waynodes_list, way_maxspeed_f, way_maxspeed_b,
                                        speed_val, oneway, lanes, lanes_f, lanes_b]
                            ways_dict[int(way_id)] = way_data

                            # add every node of the way to a list (even if they are repeated) for future manipulating, see if they are repeated is too expensive
                            for i in waynodes_list:
                                nodes_of_ways.append(i)

                            # same as dictionary, but this could be avoided
                            ways.append((int(way_id), int(start_node_id), int(end_node_id), waynodes_list, way_type,
                                         way_maxspeed))

                        waynodes_list = []
                        tag_list = []
                        way_type = ''
                        way_maxspeed = None
                        way_maxspeed_f = None
                        way_maxspeed_b = None
                        lanes_f = None
                        lanes_b = None
                        lanes = None
                        speed_val = None
                        way_check = 0

        nodes_of_ways.sort(key=float)
        # EXPORT ways_europe TO FILE
        europe_ways_df = pd.DataFrame.from_records(ways, columns = [
            "way_id", "start_node_id", "end_node_id", "list of nodes", "way_type", "way_maxspeed"
        ])
        europe_ways_df.to_csv(str(out_path)+"\europe_ways.csv", sep = ",", index = None)

        # EXPORT ways_dict TO FILE
        with open(str(out_path) + '/europe_ways_dict' + '.pkl', 'wb') as f:
            pickle.dump(ways_dict, f, pickle.HIGHEST_PROTOCOL)
    else:
        file = open(str(out_path) + "/europe_ways_dict.pkl", 'rb')
        ways_dict = pickle.load(file)

    # -----------------------------------------------------------------------------
    # NODES
    # -----------------------------------------------------------------------------
    if os.path.isfile(str(out_path) + "/europe_nodes_dict2056.pkl") == False:
        ch_border30k = gpd.read_file(
            str(shp_path) + "/bci_polygon30k_4326.shp")
        nodes_europe = {}
        i = 0

        with bz2file.open(str(out_path) + "/europe-latest_nodes.osm.bz2") as f:
            #     reading line by line the 'nodes' file created at the beginning, data for each node fulfilling the conditions are stored for the output
            for line in tqdm_notebook(f, total=lines_nodes):
                if b"<node" in line:
                    # records the attributes of the element: node_id, latitude and longitude
                    m = re.search(rb'id="(.+)" lat="([+-]?\d+(?:\.\d+)?)" lon="([+-]?\d+(?:\.\d+)?)"', line)
                    #             m = re.search(rb'id="(.+)" lat="([0-9.]+)" lon="([0-9.]+)"', line)
                    if m is not None:
                        #         this is done to take only nodes that are contained in the ways filtered in the previous step
                        #         and taking into account that nodes ids are sorted in the OSM file
                        node_sel = nodes_of_ways[i]
                        id = m.group(1).decode('utf-8')
                        if node_sel == id:
                            lonlat = float(m.group(3)), float(m.group(2))
                            # activate this(next 3 lines) if location constrain wants to be set to nodes filtering
                            in_ch = ch_border30k.contains(Point(lonlat))
                            if in_ch[0] == True:
                                nodes_europe[int(id)] = lonlat
                                # desactivate this(next 1 line) if location constrain is not needed
                                #                 nodes_europe[int(id)] = lonlat
                            # ------------------
                            if i < len(nodes_of_ways) - 1:
                                while node_sel == id:
                                    i += 1
                                    node_sel = nodes_of_ways[i]

        # EXPORT nodes_europe (dictionary) TO FILE
        with open(str(out_path) + '/europe_nodes_dict4326' + '.pkl', 'wb') as f:
            pickle.dump(nodes_europe, f, pickle.HIGHEST_PROTOCOL)

        # translate them to coordinate system 2056  from 4326
        nodes_europe_2056 = {}
        for node in list(nodes_europe):
            point4326 = Point(nodes_europe[node])
            project = partial(
                pyproj.transform,
                pyproj.Proj(init='epsg:4326'),
                pyproj.Proj(init='epsg:2056'))
            point2056 = transform(project, point4326)
            nodes_europe_2056[node] = (point2056.x, point2056.y)
        # EXPORT nodes_europe TO FILE
        with open(str(out_path) + '/europe_nodes_dict2056.pkl', 'wb') as f:
            pickle.dump(nodes_europe_2056, f, pickle.HIGHEST_PROTOCOL)
    else:
        file = open(str(out_path) + "/europe_nodes_dict2056.pkl", 'rb')
        nodes_europe_2056 = pickle.load(file)

    # DELETE WAYS WHICH ARE NOT INSIDE swissborder 30k (if only one node of the way is inside it is kept)
    if os.path.isfile(str(out_path) + "/europe_Sways_dict.pkl") == False:
        for wayid in list(ways_dict):
            nodes_list = ways_dict[wayid][2]
            for node in nodes_list:
                try:
                    Point(nodes_europe_2056[node])
                except:
                    del ways_dict[wayid]
                    break
                    # continue

        #EXPORT ways_dict TO FILE
        with open(str(out_path)+'\europe_Sways_dict.pkl', 'wb') as f:
            pickle.dump(ways_dict, f, pickle.HIGHEST_PROTOCOL)

    # -----------------------------------------------------------------------------
    # SPLIT WAYS
    # -----------------------------------------------------------------------------
    if os.path.isfile(str(out_path) + "/europe_ways_splitted_dict.pkl") == False:
    # Create dictionary that counts which nodes are part of 2 or more ways and specify how many times (i.e. node_id 1 : is in 3 ways)
        nodes_repeated = {}
        allnodes_count = {}
        waysid_per_node = {}

        for way in ways_dict:
            nodesway_list = []
            ways_data = ways_dict[way][2]
            for check_node in ways_data:
                if check_node in nodes_repeated:
                    nodes_repeated[check_node] = nodes_repeated[check_node] + 1
                    allnodes_count[check_node] = allnodes_count[check_node] + 1
                else:
                    nodes_repeated[check_node] = 1
                    allnodes_count[check_node] = 1
        # Keep repeated nodes in ways
        for key in list(nodes_repeated):
            if nodes_repeated[key] <= 1:
                del nodes_repeated[key]

        # Creates a dictinoary ('crossing_ways') with all the ways in which a node is included ( i.e. node_id: way_id1, way_id2, way_id3,...)
        in_ways_id = {}
        crossing_ways = {}
        ways_list = []

        for way_id in ways_dict:
            nodes_list = ways_dict[way_id][2]
            for node in nodes_list:
                if node in in_ways_id:
                    in_ways_id[node].append(way_id)
                    crossing_ways[node].append(way_id)
                else:
                    in_ways_id[node] = [way_id]
                    crossing_ways[node] = [way_id]
        for key in list(crossing_ways):
            ways = crossing_ways[key]
            ways_len = len(ways)
            if ways_len == 1:
                del crossing_ways[key]

        # create crossing_nodes with (wayid and every node that has an intersection(node that is part of more than 1 way))
        crossing_nodes = {}

        for way in ways_dict:
            nodes = ways_dict[way][2]
            for i in nodes:
                if i in nodes_repeated:
                    if way in crossing_nodes:
                        crossing_nodes[way].append(i)
                    else:
                        crossing_nodes[way] = [i]
                else:
                    if way not in crossing_nodes:
                        crossing_nodes[way] = []
        for way_id in ways_dict:
            nodes_list = ways_dict[way_id][2]
            num_nd = len(nodes_list)
            init_pos = 0
            count_id = 0
            node_pos_dict = {}
            if len(crossing_nodes[way_id]) > 0 and num_nd > 1:
                for i in crossing_nodes[way_id]:
                    if i not in list(node_pos_dict):
                        # this avoids errors if the same node is 2 or more times in the same way
                        node_pos_dict[i] = [[j for j, v in enumerate(nodes_list) if v == i], 0]
                    nd_pos = node_pos_dict[i][0][node_pos_dict[i][1]]
                    node_pos_dict[i][1] += 1

                    if nd_pos > 0 and nd_pos < num_nd:
                        new_nodes = nodes_list[init_pos:(nd_pos + 1)]
                        init_pos = nd_pos
                        split_ways(init_pos, nd_pos, way_id, count_id, new_nodes)
                        count_id += 1
                    elif (nd_pos == 0 or nd_pos == num_nd) and len(crossing_nodes[way_id]) == (
                            list(crossing_nodes[way_id]).index(i) + 1):
                        nd_pos = num_nd
                        new_nodes = nodes_list[init_pos:(nd_pos + 1)]
                        split_ways(init_pos, nd_pos, way_id, count_id, new_nodes)
                        count_id += 1
            elif num_nd > 1:
                split_ways(init_pos, nd_pos, way_id, count_id, nodes_list)

        # EXPORT splitted_ways_dict INTO FILE
        with open(str(out_path) + '/europe_ways_splitted_dict' + '.pkl', 'wb') as f:
            pickle.dump(splitted_ways_dict, f, pickle.HIGHEST_PROTOCOL)

        # EXPORT ways_europe TO FILE
        europe_ways_df = pd.DataFrame.from_records(ways, columns=[
            "way_id", "start_node_id", "end_node_id", "list of nodes", "way_type", "way_maxspeed"
        ])
        europe_ways_df.to_csv(str(out_path) + "\europe_ways.csv", sep=",", index=None)
    else:
        # IMPORT splitted_ways_dict
        file = open("europe_ways_splitted_dict.pkl",'rb')
        splitted_ways_dict = pickle.load(file)
        file.close()

        #splitted_ways
        with open('splitted_MTP_europe1.csv', 'r') as f:
            reader = csv.reader(f)
        splitted_ways = list(reader)

#Add coordinates of every node forming a way, not only start and end points
#and create LINESTRING with all the nodes in the way, not only start and end nodes
def lonlat_funct(new_id,nodes_list):
    nodes_lonlat = []
    for i in nodes_list:
        nodes_lonlat.append(nodes_europe[i])
#     splitted_ways_dict[new_id].append(nodes_lonlat)
    return nodes_lonlat

def ls_func(wayid,nodes_lonlat):
    line = geo.LineString(nodes_lonlat)
    return line

#Export to a csv file
splitted_ways_df = pd.DataFrame.from_records(splitted_ways, columns = [
    "new_id", "way_id", "count_id", "start_node_id", "end_node_id", "way_type",
    "maxspeed(km/h)", "nodes_list", "maxs_f","maxs_b","speed_val","oneway","lanes","lanes_f","lanes_b"
])

splitted_ways_df['nodes_lonlat'] = splitted_ways_df.apply(lambda row: lonlat_funct(row['new_id'],row['nodes_list']), axis=1)
splitted_ways_df['geometry'] = splitted_ways_df.apply(lambda row: ls_func(row['way_id'],row['nodes_lonlat']), axis=1)

# splitted_ways_df.to_csv(str(out_path)+"\splitted_MTP_europe.csv", sep = ",", index = None)

# Adds length of linestring and time spent on the way at maxspeed value
# also creates geopandas dataframe and transforms coordinates from 4326 to 2056 system

# try:
def length_func(line):
    line_length = line.length
    return line_length

def way_time_func(maxspeed,length):
    way_time = float(length)/(float(maxspeed)*1000/3600)
    return way_time

gdf = gpd.GeoDataFrame(splitted_ways_df)

# crs_source = ('+proj=longlat +datum=WGS84 +no_defs') #EPSG:4326
# crs_target = ('+proj=somerc +lat_0=46.95240555555556 +lon_0=7.439583333333333 +k_0=1 +x_0=2600000 +y_0=1200000 +ellps=bessel +towgs84=674.374,15.056,405.346,0,0,0,0 +units=m +no_defs') #EPSG:2056
# gdf.crs = crs_source
# gdf = gdf.to_crs(crs_target)
gdf.crs = {"init" : "EPSG:4326"}
gdf = gdf.to_crs({"init" : "EPSG:2056"})

gdf['length(m)'] = gdf.apply(lambda row: length_func(row['geometry']), axis=1)
gdf['time(s)'] = gdf.apply(lambda row: way_time_func(row['maxspeed(km/h)'],row['length(m)']), axis=1)

gdf[["new_id", "geometry","start_node_id","end_node_id","length(m)","time(s)"]].to_file(str(out_path)+"\gdf_MTP_europe.shp")
gdf.to_csv(str(out_path)+"\gdf_MTP_europe.csv", sep = ",", index = None)

