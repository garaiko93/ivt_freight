import pandas as pd
import pickle
import networkx as nx
import os
import math
import datetime
from scipy import spatial
from functools import partial
import pyproj
from shapely.ops import transform
import geopandas as gpd
from shapely.geometry import Point, Polygon
from progressbar import Percentage, ProgressBar, Bar, ETA


'''
02.08.2019
from now:  
    optimise routing:
        remove filter of >5000
        find missing NUTS and PLZ
CHECK:
    run again bcrossings script, to upload border crossings ( just the location)
    and pass them to this points, to see the closest (with a bound around this: 500m?)
    to then match the names to this crossing points
    deleting all the others found crossing points by deleting the new id from the graph (create new graph)
    set nuts_routing_opt code to run
    run again closest node for data manipulating AS centroids MUST be inside switzerland to do the routing correctly
    RUN osm_europe to export network within the 30k swiss border roads (secondary)
        merge network for eu123_ch4
    run again routing
    compare results
'''

# insert here parser code for terminal running code with arguments
# arguments to define:
# paths: output, network_path, border_file, bc_path
# thresholds: freight_min_value, bc_radius


def find_bc(network_path, border_file, bc_path):
    print(datetime.datetime.now(), 'Border crossing search begins ...')
    # -----------------------------------------------------------------------------
    # DEFINE OUT_PATH AND LOAD FILES
    # -----------------------------------------------------------------------------
    # network_path = 'C:/Users/Ion/IVT/OSM_python/merged_networks/ch1234'
    # out_path = 'C:/Users/Ion/IVT/OSM_python/test/bc_official'

    # border_file = 'C:/Users/Ion/IVT/OSM_python/switzerland/ch_bordercrossings/swiss_border/bci_path.shp'
    # bc_path = 'C:/Users/Ion/IVT/OSM_python/freight_data/freight/official_counting_ot.csv'

    out_path = str(network_path) + '/bc_official'
    network_files = str(network_path) + '/network_files'

    if not os.path.exists(str(out_path)):
        os.makedirs(str(out_path))
        print(datetime.datetime.now(), 'Directory created')
    else:
        print(datetime.datetime.now(), 'Directory exists')

    # import shp files with network and CH border
    ch_border = gpd.read_file(border_file)
    europe_network = gpd.read_file(str(network_files) + '/gdf_MTP_europe.shp')
    official_df = pd.read_csv(bc_path)

    print(datetime.datetime.now(), 'Nlines in ch_border: ' + str(len(ch_border)))
    print(datetime.datetime.now(), 'Nways in europe_network: ' + str(len(europe_network)))
    print(datetime.datetime.now(), 'Nbc in official_df: ' + str(len(official_df)))
    print('------------------------------------------------------------------------')

    # -----------------------------------------------------------------------------
    # FIND WAYS THAT INTERSECT CH BORDER
    # -----------------------------------------------------------------------------
    if os.path.isfile(str(out_path) + "/crossing_onlypoints.shp") is False:
        # define the line that defines the swiss border:
        # ring = LineString(list(ch_border.exterior[0].coords))
        # ring = ch_border['geometry'][0].exterior
        ring = ch_border['geometry'][0]
        points_list = []
        id_list = []
        pbar = ProgressBar(widgets=[Bar('>', '[', ']'), ' ', Percentage(), ' ', ETA()],
                           maxval=len(europe_network))
        # loops over all the ways defined in the network and keeps the coordinates and the new_id of the way crossing the ring
        print(datetime.datetime.now(), 'Searching ways from the network that intersect the Swiss border ...')
        for i in pbar(range(0, len(europe_network))):
            ls = europe_network.iloc[i]['geometry']
            point = ring.intersection(ls)
            if point.bounds != ():
                id = europe_network.iloc[i]['new_id']
                try:
                    if len(point) > 1:
                        for j in range(0, len(point)):
                            points_list.append([Point(point[j].x, point[j].y), id])
                except:
                    points_list.append([Point(point.x, point.y), id])

        print(datetime.datetime.now(), 'Process iterated over ' + str(len(europe_network)) + ' ways.')
        print(datetime.datetime.now(), 'Number of border crossings found in the network: ' + str(len(points_list)))

        #EXPORT found points TO FILES (csv and shp)
        crossing_points_df = pd.DataFrame.from_records(points_list, columns=["geometry", "new_id"])
        crossing_points_df.to_csv(str(out_path) + "/crossing_onlypoints.csv", sep=",", index=None)

        ch_bc = gpd.GeoDataFrame(crossing_points_df)
        ch_bc.to_file(str(out_path) + "/crossing_onlypoints.shp")
        print('------------------------------------------------------------------------')
    else:
        # CHECKPOINT: load file created before
        ch_bc = gpd.read_file(str(out_path) + "/crossing_onlypoints.shp")
        print(datetime.datetime.now(), 'Border crossings already found, loaded: ' + str(len(ch_bc)))
        print('------------------------------------------------------------------------')

    # -----------------------------------------------------------------------------
    # FILTER OFFICIAL_DF WITH FREIGHT BORDER CROSSINGS
    # -----------------------------------------------------------------------------
    # create dictinoary with id and border crossing point from official freight data
    bc_id = {}
    for i in range(0,len(official_df)):
        nr = official_df.iloc[i]['Nr.']
        name = official_df.iloc[i]['Name']
        bc_id[nr] = name
    len(bc_id)

    # DROP ROWS with no data or which do not overpass an input record
    droprows=[]
    #adjust this value for the filter minimum
    min_freight = 0

    for i in range(0,len(official_df)):
        if (
        (math.isnan(official_df.iloc[i]['ZV']) == True or official_df.iloc[i]['ZV'] < int(min_freight)) and
        (math.isnan(official_df.iloc[i]['QV']) == True or official_df.iloc[i]['QV'] < int(min_freight)) and
        (math.isnan(official_df.iloc[i]['TV']) == True or official_df.iloc[i]['TV'] < int(min_freight)) and
        (math.isnan(official_df.iloc[i]['BV']) == True or official_df.iloc[i]['BV'] < int(min_freight)) and
        (math.isnan(official_df.iloc[i]['Total']) == True or official_df.iloc[i]['Total'] < int(min_freight))
        ):
            droprows.append(i)
    official_df = official_df.drop(official_df.index[droprows])
    print(datetime.datetime.now(), 'Remaining border crossings in official_df after applying filter of minimum value(' +
          str(min_freight) + '): ' + str(len(official_df)))
    print('------------------------------------------------------------------------')

    # -----------------------------------------------------------------------------
    # DEFINE COORDINATES OF OFFICIAL BORDER CROSSINGS (MANUALLY)
    # -----------------------------------------------------------------------------
    # Geolocate the crossing points of the freight data to later find the closest border crossings in the created network according to the name of the official data
    # create dataframe with name of the border stations from the original data
    data = {'Nr.': list(official_df['Nr.']),
            'Name': list(official_df['Name'])
            }
    crossings = pd.DataFrame(data, columns=['Nr.', 'Name', 'lon', 'lat', 'country', 'group', 'found_bc'])
    # crossings = crossings.set_index(['Nr.', 'Name'])

    # this is the most manual part: it was checked one by one where each point is located by the name included in the original data
    # it was found that some of the names do not have results or are in other countries
    # so these NAMES are changed to the real towns names which refer to a precise coordinate of the crossing which provide a correct coordinate to find nearest neighbours
    def supl_data(name, data2):
        crossings.loc[crossings['Name'] == name, 'lon'] = data2[0]
        crossings.loc[crossings['Name'] == name, 'lat'] = data2[1]
        crossings.loc[crossings['Name'] == name, 'country'] = data2[2]
        crossings.loc[crossings['Name'] == name, 'group'] = data2[3]
        crossings.loc[crossings['Name'] == name, 'found_bc'] = data2[4]

        # crossings.loc[[1030], 'lon']
        # crossings.loc[[name], 'lon'] = data2[0]
        # crossings.loc[[name], 'lat'] = data2[1]
        # crossings.loc[[name], 'country'] = data2[2]
        # crossings.loc[[name], 'group'] = data2[3]
        # crossings.loc[[name], 'found_bc'] = data2[4]

    def add_bc_lonlat():
        supl_data('Goumois', [47.261682, 6.951284, 'FR', 0, 1])
        supl_data('Damvant', [47.372073, 6.883789, 'FR', 0, 1])
        supl_data('Fahy', [47.423343, 6.940931, 'FR', 0, 1])
        supl_data('Boncort', [47.493487, 6.985808, 'FR', 0, 1])
        supl_data('Beurnévesin', [47.492841, 7.136547, 'FR', 0, 0])  # not found
        supl_data('Boncourt-Ville', [47.502806, 7.012420, 'FR', 0, 1])
        supl_data('Miécourt', [47.441160, 7.183286, 'FR', 0, 1])
        supl_data('Charmoille', [47.426500, 7.245687, 'FR', 0, 1])
        supl_data('Lucelle', [47.421478, 7.245972, 'FR', 0, 1])
        supl_data('Basel/Hüningen', [47.576310, 7.575331, 'FR', 1, 1])
        supl_data('Roggenburg / Neumühle', [47.440156, 7.327425, 'FR', 0, 1])
        supl_data('Kleinlützel', [47.432415, 7.381577, 'FR', 0, 1])
        supl_data('Rodersdorf - Leymenstrasse', [47.486521, 7.464715, 'FR', 0, 1])
        supl_data('Burg', [47.458129, 7.439031, 'FR', 0, 0])  # not found
        supl_data('Flüh', [47.488701, 7.498179, 'FR', 0, 0])  # not found
        supl_data('Benken', [47.653579, 8.652355, 'DE', 0, 0])  # not found
        supl_data('Basel - Lisbüchel', [47.577073, 7.571968, 'FR', 1, 1])
        supl_data('Allschwil I', [47.555583, 7.536505, 'FR', 0, 1])
        supl_data('Basel - Hegenheimerstrasse', [47.565069, 7.557385, 'FR', 1, 1])  # not found (tertiary road)
        supl_data('Basel - Burgfelderstrasse', [47.572405, 7.557012, 'FR', 1, 0])
        supl_data('Basel - St. Johann-Hüningerstr.', [47.573338, 7.577322, 'FR', 1, 0])  # not found
        supl_data('Basel - Hiltalingerstrasse', [47.589007, 7.593192, 'DE', 1, 1])
        supl_data('Basel - Freiburgerstrasse', [47.581305, 7.604434, 'DE', 1, 1])
        supl_data('Riehen', [47.595272, 7.655513, 'FR', 3, 1])
        supl_data('Riehen - Weilstrasse', [47.592192, 7.642485, 'DE', 3, 1])
        supl_data('Riehen - Inzlingerstrasse', [47.585528, 7.672028, 'DE', 3, 0])  # not found
        supl_data('Riehen - Grenzacherstrasse', [47.562312, 7.634682, 'DE', 3, 1])
        supl_data('Ubf Basel/Weil', [47.581342, 7.602747, 'DE', 0, 0])  # not found
        supl_data('Rheinfelden', [47.555188, 7.789608, 'DE', 0, 0])
        supl_data('Stein/Bad Säckingen', [47.546196, 7.949324, 'DE', 0, 1])
        supl_data('Laufenburg', [47.561695, 8.074637, 'DE', 0, 1])
        supl_data('Basel/Weil Autobahn', [47.586065, 7.602151, 'DE', 1, 1])
        supl_data('Basel/St.Louis Autobahn', [47.575299, 7.562977, 'FR', 1, 1])
        supl_data('Rheinfelden Autobahn', [47.548419, 7.758308, 'DE', 0, 1])
        supl_data('Koblenz', [47.608510, 8.233268, 'DE', 0, 1])
        supl_data('Zurzach', [47.586129, 8.302407, 'DE', 0, 1])
        supl_data('Kaiserstuhl', [47.569942, 8.419021, 'DE', 0, 0])  # not found
        supl_data('Trasadingen', [47.662112, 8.432025, 'DE', 4, 1])
        supl_data('Osterfingen', [47.657180, 8.466042, 'DE', 4, 0])  # not found
        supl_data('Schleitheim', [47.749054, 8.456390, 'DE', 4, 1])
        supl_data('Wasterkingen', [47.586690, 8.465526, 'DE', 4, 1])
        supl_data('Wil Grenze', [47.610611, 8.477679, 'DE', 5, 1])
        supl_data('Rafz Grenze', [47.620231, 8.561097, 'DE', 5, 0])
        supl_data('Rafz - Solgen', [47.614016, 8.570886, 'DE', 5, 1])
        supl_data('Rheinau', [47.647638, 8.602823, 'DE', 5, 0])  # not found
        supl_data('Neuhausen am Rheinfall', [47.669325, 8.595693, 'DE', 4, 1])
        supl_data('Bargen', [47.801884, 8.575616, 'CH', 4, 1])  # not found
        supl_data('Neudörflingen', [47.716870, 8.735802, 'DE', 4, 0])  # not found
        supl_data('Dörflingen - Laag', [47.695159, 8.726867, 'DE', 4, 1])
        supl_data('Ramsen', [47.712053, 8.822262, 'DE', 6, 1])
        supl_data('Buch - Grenze', [47.726685, 8.785883, 'DE', 6, 0])  # not found
        supl_data('Ramsen - Dorf', [47.700409, 8.798507, 'DE', 6, 1])
        supl_data('Stein am Rhein', [47.660416, 8.875806, 'DE', 0, 1])
        supl_data('Thayngen', [47.740526, 8.719062, 'DE', 4, 1])
        supl_data('Thayngen - Schlatt', [47.662780, 8.702995, 'FR', 4, 0])  # not found
        supl_data('Hofen', [47.786628, 8.680919, 'DE', 4, 1])
        supl_data('Kreuzlingen - Konstanz', [47.656146, 9.169540, 'DE', 7, 1])
        supl_data('Kreuzlingen - Hauptstrasse', [47.655372, 9.173241, 'DE', 7, 0])  # not found
        supl_data('Kreuzlingen Autobahn', [47.661832, 9.161328, 'DE', 7, 1])
        supl_data('Tägerwilen', [47.663132, 9.159908, 'DE', 7, 0])  # not found
        supl_data('Romanshorn', [47.565192, 9.366304, 'CH', 0, 0])  # not found
        supl_data('Rheineck', [47.463695, 9.594341, 'AU', 8, 1])
        supl_data('Au (SG)', [47.431232, 9.645630, 'AU', 8, 1])
        supl_data('St. Margrethen Freilager', [47.456222, 9.639271, 'AU', 8, 1])
        supl_data('Widnau', [47.408787, 9.651907, 'AU', 8, 0])  # not found
        supl_data('Schmitter', [47.394438, 9.669236, 'AU', 8, 1])
        supl_data('Kriessern', [47.357518, 9.613228, 'AU', 8, 1])
        supl_data('Oberriet', [47.306132, 9.570565, 'AU', 8, 1])
        supl_data('Ruggell FL', [47.243606, 9.520346, 'LIE', 9, 1])
        # supl_data('Schaanwald',[47.218100, 9.570216,'LIE',9,0]) #not found
        supl_data('Schaanwald', [47.169089, 9.489238, 'LIE', 9, 1])
        supl_data('Martina', [46.884769, 10.464907, 'AU', 10, 1])
        supl_data('Müstair', [46.635305, 10.458483, 'AU', 10, 1])
        supl_data('La Drossa', [46.623815, 10.193153, 'IT', 0, 1])
        supl_data('Campocologno', [46.230386, 10.145480, 'IT', 0, 1])
        supl_data('La Motta', [46.440873, 10.056076, 'IT', 0, 1])
        supl_data('Castasegna', [46.332815, 9.513727, 'IT', 0, 1])
        supl_data('Splügen', [46.505496, 9.330320, 'IT', 0, 1])
        supl_data('Diepoldsau', [47.377660, 9.670672, 'IT', 0, 1])
        supl_data('Gandria', [46.016840, 9.022388, 'IT', 0, 1])
        supl_data('Brusata', [45.840341, 8.959557, 'IT', 2, 1])
        supl_data('Ponte Tresa', [45.966996, 8.858952, 'IT', 0, 1])
        supl_data('Fornasette', [45.993012, 8.787757, 'IT', 0, 1])
        supl_data('Chiasso-Brogeda Merci', [45.834861, 9.037154, 'IT', 2, 1])
        supl_data('Chiasso-Brogeda Autostrada', [45.839330, 9.035400, 'IT', 2, 1])
        supl_data('Chiasso Strada Viaggiatori', [45.832050, 9.034515, 'IT', 2, 0])
        supl_data('Stabio Confine', [45.841346, 8.913843, 'IT', 2, 1])
        supl_data('Arzo', [45.874040, 8.933621, 'IT', 2, 0])  # not found
        supl_data('Madonna di Ponte', [46.101832, 8.696770, 'IT', 0, 1])
        supl_data('Dirinella', [46.104118, 8.756328, 'IT', 0, 1])
        supl_data('Gondo', [46.196626, 8.151436, 'IT', 0, 1])
        supl_data('St. Gingolph', [46.393369, 6.804346, 'FR', 0, 1])
        supl_data('Le Châtelard', [46.677033, 6.978262, 'CH', 0, 0])
        supl_data('Morgins', [46.249667, 6.845849, 'FR', 0, 1])
        supl_data('Crassier', [46.374062, 6.163071, 'FR', 0, 1])
        supl_data('Chavannes-de-Bogis', [46.345988, 6.151822, 'FR', 0, 1])
        supl_data('La Cure', [46.464185, 6.073394, 'FR', 0, 1])
        supl_data('Charbonnières', [46.680390, 6.268850, 'FR', 0, 0])
        supl_data('Le Brassus', [46.546160, 6.155305, 'FR', 0, 1])  # not found
        supl_data('Vallorbe - Route', [46.731546, 6.384708, 'FR', 0, 1])
        supl_data('L\'Auberson', [46.824107, 6.439352, 'FR', 0, 1])
        supl_data('Les Verrières', [46.900768, 6.457935, 'FR', 0, 1])
        supl_data('Col France', [47.051308, 6.719108, 'FR', 0, 1])
        supl_data('Les Brenets - Route', [47.063219, 6.694539, 'FR', 0, 0])  # not found
        supl_data('Biaufond', [47.164128, 6.858230, 'FR', 0, 1])
        supl_data('Grand St. Bernard Tunnel', [45.864522, 7.172663, 'IT', 0, 1])
        supl_data('Hermance', [46.302529, 6.247461, 'FR', 11, 1])
        supl_data('Moillesulaz', [46.192201, 6.206615, 'FR', 11, 1])
        supl_data('Mon-Idée', [46.201409, 6.224489, 'FR', 11, 0])  # not found
        supl_data('Anières', [46.272809, 6.242745, 'FR', 11, 1])
        supl_data('Veigy', [46.262631, 6.249612, 'FR', 11, 0])  # not found
        supl_data('Thônex - Vallard', [46.188347, 6.202957, 'FR', 11, 1])
        supl_data('Veyrier', [46.166369, 6.188467, 'FR', 11, 1])
        supl_data('Bardonnex', [46.148438, 6.095631, 'FR', 11, 1])
        supl_data('Chancy II', [46.144285, 5.964493, 'FR', 11, 1])
        supl_data('Perly', [46.151694, 6.089973, 'FR', 11, 1])
        supl_data('Troinex', [46.154518, 6.161268, 'FR', 11, 0])  # not found
        supl_data('Croix-de-Rozon', [46.143713, 6.138122, 'FR', 11, 1])
        supl_data('Soral II', [46.142602, 6.046963, 'FR', 11, 0])  # not found
        supl_data('Mategnin', [46.243700, 6.092258, 'FR', 11, 0])  # not found
        supl_data('Chancy I', [46.133639, 5.977528, 'FR', 11, 0])  # not found
        supl_data('Meyrin', [46.235006, 6.050086, 'FR', 11, 1])
        supl_data('Ferney-Voltaire', [46.248642, 6.120772, 'FR', 11, 1])
        supl_data('Bossy', [46.286929, 6.104580, 'FR', 11, 0])  # not found
        supl_data('Sauverny', [46.311602, 6.120305, 'FR', 11, 0])  # not found
        supl_data('Landecy', [46.141336, 6.131986, 'FR', 11, 0])  # not found

    add_bc_lonlat()
    print(datetime.datetime.now(), 'Number of crossings in df: ' + str(len(crossings)))
    crossings.head()

    # convert coordinates to 2056 coordinate system
    def geolocate(x,y):
        #change coordinates system
        point4326 = Point(x, y)
        project = partial(
            pyproj.transform,
            pyproj.Proj('epsg:4326'),
            pyproj.Proj('epsg:2056'))
        point2056 = transform(project, point4326)
        return pd.Series([point2056])
        # return point2056

    crossings[['geometry']] = crossings.apply(lambda row: geolocate(row['lon'],row['lat']),axis=1)
    crossings = pd.merge(official_df, crossings[['Nr.','geometry','country','group','found_bc']], how='inner', on='Nr.')
    # this are merged to get all the freight data for each geometry point
    print(datetime.datetime.now(), len(crossings))
    print('------------------------------------------------------------------------')

    # -----------------------------------------------------------------------------
    # FIND CLOSEST NETWORK BORDER CROSSING
    # -----------------------------------------------------------------------------
    # Match the closest border crossings found in the network in a radius of 6 km from each of the crossings of the original data
    # Starting by creating a tree with the coordinates of all border crossings found with the official data NAMES in the previous
    G_lonlat = []
    for i in range(0, len(ch_bc)):
        G_lonlat.append((ch_bc.iloc[i]['geometry'].x,
                         ch_bc.iloc[i]['geometry'].y))
    tree = spatial.KDTree(G_lonlat)
    # droprows = []

    def closest_bc(bc):
    # For each official border crossing defined in the step before the closest neighbour is found (later filtered if they are above 6km distance)
    # The important output of this function is the new_ids of the ways that are matched to the official border crossings (stored later as wayid_by_cp), so it can be checked on the routing
        new_ids = []
        cp = []
        point1 = (bc.x, bc.y)
        nn = tree.query(point1)
        coord = G_lonlat[nn[1]]
        distance = Point(point1).distance(Point(coord))
        for j in range(0, len(ch_bc)):
            point2 = ch_bc.iloc[j]['geometry']
            new_id = ch_bc.iloc[j]['new_id']
            # Last, for each closest crossing in the network, a grouping of the crossings in a radius of 500 m is done,
            # so also different lines of the same way are matched with the same border crossing
            if Point(coord).distance(point2) < 100:
                new_ids.append(new_id)
                cp.append((point2.x, point2.y))
        return pd.Series([coord, distance, new_ids, cp])

    crossings[['closest_bc', 'distance(m)', 'new_ids', 'cp_coords']] = crossings.apply(
        lambda row: closest_bc(row['geometry']), axis=1)
    crossings = crossings.sort_values('distance(m)', ascending=False)
    # crossings = crossings[crossings['distance(m)']<6000] #filter of 6k
    # crossings = crossings[crossings['found_bc']== 1] #filter of found bc
    # crossings = crossings.sort_values('group', ascending=False)

    # export crossings info to a csv and shp file
    crossings.to_csv(str(out_path) + "/crossings_unofficial.csv", sep=",", index=None,
                     encoding='latin1')
    gdf = gpd.GeoDataFrame(crossings)
    gdf[['Nr.', 'Name', 'geometry']].to_file(str(out_path) + "/crossings_unofficial.shp")

    print(datetime.datetime.now(), 'Crossing points in crossings dataframe: ' + str(len(crossings)))
    print('------------------------------------------------------------------------')

    # For an easier later comparison, a dictionary is created containing data of the border crossings, this will be loaded in the routing code
    wayid_by_cp = {}
    coord_list = []
    rep_ids = []
    for i in range(0, len(crossings)):
        name = crossings.iloc[i]['Name']
        station_id = crossings.iloc[i]['Nr.']
        new_ids = crossings.iloc[i]['new_ids']
        o_point = crossings.iloc[i]['geometry']
        coords = crossings.iloc[i]['cp_coords']
        for id in new_ids:
            wayid_by_cp[id] = name
            if id in list(wayid_by_cp):
                rep_ids.append(id)
        # this loop stores the info in a list for a later creation of the 'bc_df' DataFrame
        for coord in coords:
            j = coords.index(coord)
            distance = Point(coord).distance(o_point)
            new_id = crossings.iloc[i]['new_ids'][j]
            coord_list.append([station_id, name, new_id, distance, Point(coord)])

    # EXPORT nuts_centroid_dict TO FILE
    with open(str(out_path) + '/wayidbycp_dict' + '.pkl', 'wb') as f:
        pickle.dump(wayid_by_cp, f, pickle.HIGHEST_PROTOCOL)
    print(datetime.datetime.now(),  'New_ids in wayid_by_cp: ' + str(len(wayid_by_cp)))
    print('------------------------------------------------------------------------')

    # dataframe bc_df contains all the information of the final border crossings with all the matches from the network
    bc_df = pd.DataFrame.from_records(coord_list, columns=[
        "Nr.", "Name", "new_id", "distance(m)", "geometry"
    ])
    bc_df = bc_df.sort_values(by=['distance(m)'], ascending=False)
    bc_df.to_csv(str(out_path) + "/bc_df.csv", sep=",", index=None, encoding='latin1')

    gdf = gpd.GeoDataFrame(bc_df)
    gdf.to_file(str(out_path) + "/bc_df.shp", encoding='latin1')

    print(datetime.datetime.now(), 'Border crossings in bc_df: ' + str(len(bc_df)))
    print(datetime.datetime.now(), 'New_ids repeated in rep_ids: ' + str(len(rep_ids)))
    print('------------------------------------------------------------------------')

    # -----------------------------------------------------------------------------
    # MODIFY GRAPH, REMOVE EDGES THAT ARE NOT IN ELECTED CROSSING POINTS
    # -----------------------------------------------------------------------------
    # IMPORT G original graph
    # (not the one with the largest island as this procedure may change this largest island so the largest network will be taken later from this output)
    G = nx.read_gpickle(str(network_files) + "/eu_network_graph_bytime.gpickle")
    print(datetime.datetime.now(), 'G graph (Nnodes/Nedges): '+ str(G.number_of_nodes()) + '/' + str(G.number_of_edges()))

    # # IMPORT nodes_europe
    file = open(str(network_files) + "/europe_ways_splitted_dict.pkl",'rb')
    europe_ways_splitted_dict = pickle.load(file)
    file.close()
    print(datetime.datetime.now(), 'Nways in europe_ways_splitted_dict: ' + str(len(europe_ways_splitted_dict)))

    # DELETE ways that contain border crossings not considered for the routing
    all_newids = list(ch_bc.new_id)  # all crossings found in the network
    elected_newids = list(
        bc_df.new_id)  # the ones that have passed all the filters and match the official border crossings from the freight data
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


#CHECK WHICH elected crossings ARE NOT IN THE GRAPH (it could be because of islands)
# new_ids=nx.get_edge_attributes(G,'new_id')
# for id in elected_newids:
#     o_node = europe_ways_splitted_dict[id][2][0]
#     d_node = europe_ways_splitted_dict[id][2][-1]
#     if (G.has_edge(int(o_node), int(d_node)))==False:
#         print(id, int(o_node), int(d_node))

# CREATE SHP FILE WITH POLYGON OF SWISS BORDER WITH 30KM OFFSET
# ch_poly = gpd.read_file(str(in_path) + "\switzerland\ch_bordercrossings\swiss_border\\bci_polygon.shp")
# polygon = ch_poly['geometry'][0]
# import matplotlib.pyplot as plt
# from shapely.geometry.polygon import LinearRing
#
# def plot_line(ax, ob, color):
#     x, y = ob.xy
#     ax.plot(x, y, color=color, alpha=0.7, linewidth=3, solid_capstyle='round', zorder=2)
#
#
# poly_line = LinearRing(polygon.exterior)
# poly_line_offset = poly_line.buffer(30000, resolution=16, join_style=2, mitre_limit=1)
#
# a = [(Polygon(list(poly_line_offset.exterior.coords)))]
# df = pd.DataFrame(a)
# geometry = Polygon(poly_line_offset.exterior)
# gpd.GeoDataFrame(df)
# gdf.crs = {"init": "EPSG:2056"}
# gdf = gdf.to_crs({"init": "EPSG:4326"})
# gdf.to_file(r"C:\Users\Ion\IVT\OSM_python\switzerland\ch_bordercrossings\swiss_border\bci_polygon30k_4326.shp",
#             encoding='latin1')
#
# fig = plt.figure()
# ax = fig.add_subplot(111)
# plot_line(ax, poly_line, "blue")
# plot_line(ax, poly_line_offset.exterior, "green")
# plt.show()