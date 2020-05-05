import pandas as pd
import pickle
import os
import datetime


def data_grouping(network_path):
    # network_path = 'C:/Users/Ion/IVT/OSM_python/networks/eu123ch4567'
    out_path = str(network_path) + '/comparison'
    routing_path = str(network_path) + '/routing_results'

    # Check if out_path exists or create it
    if not os.path.exists(str(out_path)):
        os.makedirs(str(out_path))
        print('Directory created')
    else:
        print('Directory exists, change name of attempt output directory')

    # LOAD routing results
    abs_routing_df = pd.read_csv(str(routing_path) + "/eu_routing_df.csv", encoding='latin1')
    print(datetime.datetime.now(), 'Nbc in abs_routing_df: ' + str(len(abs_routing_df)))

    # LOAD official routing data
    # official_df = pd.read_csv(r"C:\Users\Ion\IVT\OSM_python\freight_data\freight\official_counting_o.csv")
    official_df = pd.read_csv(str(routing_path) + "/real_routing_df.csv", encoding='latin1')
    # official_df = official_df.rename(columns={"ZV_o": "ZV", "QV_o": "QV", "TV_o": "TV", "BV_o": "BV"})
    official_df.columns = ['Nr', 'Name', 'ZV_o', 'QV_o', 'TV_o', 'in_TV', 'out_TV', 'BV_o', 'in_BV', 'out_BV']
    print(datetime.datetime.now(), 'Nbc in official_df: ' + str(len(official_df)))

    # LOAD official routing data
    bc_data = pd.read_csv(str(network_path) + "/bc_official/crossings_unofficial.csv", encoding='latin1')
    bc_data.columns = ['Nr', 'Name', 'ZV', 'QV', 'TV', 'BV', 'Total', 'geometry', 'country',
       'group', 'found_bc', 'closest_bc', 'distance(m)', 'new_ids',
       'cp_coords']
    print(datetime.datetime.now(), 'Nbc in bc_data: ' + str(len(bc_data)))

    # create dictinoary with Nr and border crossing information
    group_dict = {}
    for i in range(0, 13):
        # group_list = bc_data[bc_data['group'] == i].Nr.to_list()
        group_list = list(bc_data[bc_data['group'] == i]['Nr'])
        group_dict[i] = group_list

    bc_id = {}
    for i in range(0, len(bc_data)):
        #     try:
        nr = int(bc_data.iloc[i]['Nr'])

        s = bc_data.iloc[i]['geometry']
        a = s[s.find("(") + 1:s.find(")")]
        x = a[a.find("'") + 1:a.find(" ")]
        y = a[a.find(" ") + 1:a.find("'")]
        bc_id[str(nr)] = [bc_data.iloc[i]['Name'],
                          bc_data.iloc[i]['country'],
                          bc_data.iloc[i]['group'],
                          bc_data.iloc[i]['found_bc'],
                          bc_data.iloc[i]['closest_bc'],
                          x,
                          y]

    bc_id[float('nan')] = 0
    bc_id['3070'] = 0  # not defined border crossing (the only one)
    bc_id[''] = 0

    bc_id['1'] = ['Basel', group_dict[1]]
    bc_id['2'] = ['Chiasso', group_dict[2]]
    bc_id['3'] = ['Riehen', group_dict[3]]
    bc_id['4'] = ['Schaffhausen', group_dict[4]]
    bc_id['5'] = ['Rafz', group_dict[5]]
    bc_id['6'] = ['Ramsen', group_dict[6]]
    bc_id['7'] = ['Konstanze', group_dict[7]]
    bc_id['8'] = ['LIE-Konstanze lake', group_dict[8]]
    bc_id['9'] = ['LIE', group_dict[9]]
    bc_id['10'] = ['EastSouth_AU', group_dict[10]]
    bc_id['11'] = ['Geneve', group_dict[11]]
    bc_id['12'] = ['others', group_dict[12]]
    # bc_id_inv = {v: k for k, v in bc_id.items()}

    # EXPORT ways_dict TO FILE
    with open(str(out_path) + "/bc_id.pkl", 'wb') as f:
        pickle.dump(bc_id, f, pickle.HIGHEST_PROTOCOL)

    print(len(bc_id))

    def name(nr):
        name = bc_id[str(int(float(nr)))][0]
        return name

    # create grouped dataframes
    with_groups = bc_data[bc_data['group'] != 0]
    with_groups = with_groups.groupby(['group']).sum()
    with_groups['Nr'] = with_groups.index
    print(with_groups.found_bc.sum())
    print(len(with_groups))

    # create dataframe without group and with bc found
    no_groups = bc_data[(bc_data['group'] == 0) & (bc_data['found_bc'] == 1)]
    # no_groups = bc_data[(bc_data['group']==0)]
    no_groups = no_groups[['Nr', 'ZV', 'QV', 'TV', 'BV', 'Total', 'found_bc', 'distance(m)']]
    print(len(no_groups))

    # merge both
    groups = pd.concat([with_groups, no_groups])
    groups['Name'] = groups.apply(lambda row: name(row['Nr']), axis=1)

    groups.to_csv(str(out_path) + "/groups.csv", sep=",", index=None, encoding='latin1')
    print(len(groups))
    groups.head()

    # add groups to official data frame and group them
    def add_group(nr, zv, qv, tv, bv):
        total = zv + qv + tv + bv
        try:
            group = bc_id[str(nr)][2]
            found_bc = bc_id[str(nr)][3]
        except:
            group = 0
            found_bc = 0
        return pd.Series([group, found_bc, total], index=['group', 'found_bc', 'total'])

    official_df[['group', 'found_bc', 'total']] = official_df.apply(
        lambda row: add_group(row['Nr'], row['ZV_o'], row['QV_o'], row['TV_o'], row['BV_o']), axis=1)
    official_df = official_df[['Nr', 'ZV_o', 'QV_o', 'TV_o', 'BV_o', 'group', 'found_bc', 'total']]
    official_df.head(10)

    # create grouped dataframes
    with_groups = official_df[(official_df['group']) != 0.0]
    with_groups = with_groups.groupby(['group']).sum()
    with_groups['Nr'] = with_groups.index
    print(with_groups.found_bc.sum())
    print(len(with_groups))

    # create dataframe without group and with bc found
    no_groups = official_df[(official_df['group'] == 0.0) & (official_df['found_bc'] == 1.0)]
    # no_groups = official_df[(official_df['group']==0.0)]
    no_groups = no_groups[['Nr', 'ZV_o', 'QV_o', 'TV_o', 'BV_o', 'total', 'found_bc']]
    print(len(no_groups))

    # merge both
    groups_o = pd.concat([with_groups, no_groups], sort=False)
    groups_o['Name'] = groups_o.apply(lambda row: name(row['Nr']), axis=1)
    groups_o.to_csv(str(out_path) + "/groups_o.csv", sep=",", index=None, encoding='latin1')



# # MERGE NETWORKS FILES
# in_path = r"C:/Users/Ion/IVT/OSM_python"
#
# # check output directory
# attempt = "eu123_ch4/2_routing"
# out_path = str(attempt) + "/comparison"
# network_path = str(in_path) + "/freight_data/" + str(attempt)
#
#
#
#
# # LOAD routing results
# abs_routing_df = pd.read_csv(str(comp_path) + "/eu_routing_df.csv",encoding='latin1')
# print('Nbc in abs_routing_df: '+ str(len(abs_routing_df)))
#
# # LOAD official routing data
# # official_df = pd.read_csv(r"C:\Users\Ion\IVT\OSM_python\freight_data\freight\official_counting_o.csv")
# official_df = pd.read_csv(str(comp_path) + "/real_routing_df.csv", encoding='latin1')
# # official_df = official_df.rename(columns={"ZV_o": "ZV", "QV_o": "QV", "TV_o": "TV", "BV_o": "BV"})
# official_df.columns = ['Nr','Name','ZV_o','QV_o','TV_o','in_TV','out_TV', 'BV_o','in_BV','out_BV']
# print('Nbc in official_df: '+ str(len(official_df)))
# official_df.head()
#
# # LOAD official routing data
# bc_data = pd.read_csv(str(in_path) + "/freight_data/freight/crossings_unofficial.csv", encoding='latin1')
# print('Nbc in bc_data: '+ str(len(bc_data)))
# bc_data.head()

# # create dictinoary with Nr and border crossing information
# group_dict = {}
# for i in range(0, 12):
#     group_list = bc_data[bc_data['group'] == i].Nr.to_list()
#     group_dict[i] = group_list
#
# bc_id = {}
# for i in range(0, len(bc_data)):
#     #     try:
#     nr = int(bc_data.iloc[i]['Nr'])
#
#     s = bc_data.iloc[i]['geometry']
#     a = s[s.find("(") + 1:s.find(")")]
#     x = a[a.find("'") + 1:a.find(" ")]
#     y = a[a.find(" ") + 1:a.find("'")]
#     bc_id[str(nr)] = [bc_data.iloc[i]['Name'],
#                       bc_data.iloc[i]['country'],
#                       bc_data.iloc[i]['group'],
#                       bc_data.iloc[i]['found_bc'],
#                       bc_data.iloc[i]['closest_bc'],
#                       x,
#                       y]
#
# bc_id[float('nan')] = 0
# bc_id['3070'] = 0  # not defined border crossing (the only one)
# bc_id[''] = 0
#
# bc_id['1'] = ['Basel', group_dict[1]]
# bc_id['2'] = ['Chiasso', group_dict[2]]
# bc_id['3'] = ['Riehen', group_dict[3]]
# bc_id['4'] = ['Schaffhausen', group_dict[4]]
# bc_id['5'] = ['Rafz', group_dict[5]]
# bc_id['6'] = ['Ramsen', group_dict[6]]
# bc_id['7'] = ['Konstanze', group_dict[7]]
# bc_id['8'] = ['LIE-Konstanze lake', group_dict[8]]
# bc_id['9'] = ['LIE', group_dict[9]]
# bc_id['10'] = ['EastSouth_AU', group_dict[10]]
# bc_id['11'] = ['Geneve', group_dict[11]]
# # bc_id_inv = {v: k for k, v in bc_id.items()}
#
# # EXPORT ways_dict TO FILE
# with open(str(in_path) + "/freight_data/freight/bc_id.pkl", 'wb') as f:
#     pickle.dump(bc_id, f, pickle.HIGHEST_PROTOCOL)
#
# print(len(bc_id))
#
# def name(nr):
#     name = bc_id[str(int(float(nr)))][0]
#     return name
#
# # create grouped dataframes
# with_groups = bc_data[bc_data['group']!=0]
# with_groups =with_groups.groupby(['group']).sum()
# with_groups['Nr'] = with_groups.index
# print(with_groups.found_bc.sum())
# print(len(with_groups))
#
# # create dataframe without group and with bc found
# no_groups = bc_data[(bc_data['group']==0)&(bc_data['found_bc']==1)]
# # no_groups = bc_data[(bc_data['group']==0)]
# no_groups = no_groups[['Nr', 'ZV', 'QV', 'TV', 'BV', 'Total', 'found_bc', 'distance(m)']]
# print(len(no_groups))
#
# # merge both
# groups = pd.concat([with_groups, no_groups])
# groups['Name'] = groups.apply(lambda row: name(row['Nr']),axis=1)
#
# groups.to_csv(str(out_path)+"/groups.csv", sep=",", index=None, encoding='latin1')
# print(len(groups))
# groups.head()
#
#
# # add groups to official data frame and group them
# def add_group(nr, zv, qv, tv, bv):
#     total = zv+qv+tv+bv
#     try:
#         group = bc_id[str(nr)][2]
#         found_bc = bc_id[str(nr)][3]
#     except:
#         group = 0
#         found_bc = 0
#     return pd.Series([group,found_bc, total], index = ['group','found_bc','total'])
#
# official_df[['group','found_bc','total']] = official_df.apply(lambda row: add_group(row['Nr'],row['ZV_o'],row['QV_o'],row['TV_o'],row['BV_o']),axis=1)
# official_df = official_df[['Nr', 'ZV_o', 'QV_o', 'TV_o', 'BV_o','group','found_bc','total']]
# official_df.head(10)
#
#
# # create grouped dataframes
# with_groups = official_df[(official_df['group'])!=0.0]
# with_groups =with_groups.groupby(['group']).sum()
# with_groups['Nr'] = with_groups.index
# print(with_groups.found_bc.sum())
# print(len(with_groups))
#
# # create dataframe without group and with bc found
# no_groups = official_df[(official_df['group']==0.0)&(official_df['found_bc']==1.0)]
# # no_groups = official_df[(official_df['group']==0.0)]
# no_groups = no_groups[['Nr', 'ZV_o', 'QV_o', 'TV_o', 'BV_o', 'total', 'found_bc']]
# print(len(no_groups))
#
# # merge both
# groups_o = pd.concat([with_groups,no_groups],sort=False)
# groups_o['Name'] = groups_o.apply(lambda row: name(row['Nr']), axis=1)
# groups_o.to_csv(str(out_path)+"/groups_o.csv", sep=",", index=None, encoding='latin1')

# print(len(groups_o))
# groups_o.head()