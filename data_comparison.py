import pandas as pd
import pickle
import datetime
import os
import math

def result_comparison(network_path):
    network_path = 'C:/Users/Ion/IVT/OSM_python/networks/eu123ch4567'
    out_path = str(network_path) + '/comparison'
    # routing_path = str(network_path) + '/routing_results'

    # LOAD routing results
    # abs_routing_df = pd.read_csv(str(network_path) + "\\abs_routing_df.csv",encoding='latin1')
    # abs_routing_df = pd.read_csv(str(network_path) + "\eu_routing_df.csv",encoding='latin1')
    abs_routing_df = pd.read_csv(str(out_path) + "/groups.csv", encoding='latin1')
    # abs_routing_df = pd.read_csv(str(network_path) + "\\real_routing_df.csv",encoding='latin1')
    print('Nbc in abs_routing_df: ' + str(len(abs_routing_df)))
    # abs_routing_df.head()

    # LOAD official routing data
    # official_df = pd.read_csv(r"C:\Users\Ion\IVT\OSM_python\freight_data\freight\official_counting_o.csv")
    # official_df = pd.read_csv(str(network_path) + "\\real_routing_df.csv", encoding='latin1')
    official_df = pd.read_csv(str(out_path) + "/groups_o.csv", encoding='latin1')
    # official_df = official_df.rename(columns={"ZV_o": "ZV", "QV_o": "QV", "TV_o": "TV", "BV_o": "BV"})
    # official_df.columns = ['Nr.','Name','ZV_o','QV_o','TV_o','in_TV','out_TV', 'BV_o','in_BV','out_BV']
    print('Nbc in official_df: ' + str(len(official_df)))
    # official_df.head()


    # DROP ROWS with no data
    droprows=[]
    threshold = 0
    for i in range(0,len(official_df)):
        if (
        (math.isnan(official_df.iloc[i]['ZV_o']) == True or official_df.iloc[i]['ZV_o'] <threshold) and
        (math.isnan(official_df.iloc[i]['QV_o']) == True or official_df.iloc[i]['QV_o'] <threshold) and
        (math.isnan(official_df.iloc[i]['TV_o']) == True or official_df.iloc[i]['TV_o'] <threshold) and
        (math.isnan(official_df.iloc[i]['BV_o']) == True or official_df.iloc[i]['BV_o'] <threshold)
        ):
            droprows.append(i)
    official_df=official_df.drop(official_df.index[droprows])
    print(len(official_df))
    official_df.head()

    # comparison = pd.merge(official_df[['Nr.','Name','ZV_o','QV_o','TV_o','BV_o']], abs_routing_df[['Name','ZV','QV','in_TV','out_BV']], how='inner', on='Name')
    # comparison = pd.merge(official_df[['Nr.','Name','ZV_o','QV_o','TV_o','BV_o', 'Leer']], abs_routing_df[['Name','ZV','QV','in_TV','in_BV']], how='inner', on='Name')
    # comparison = pd.merge(official_df[['Nr.','Name','ZV_o','QV_o','TV_o','BV_o']], abs_routing_df[['Name','ZV','QV','out_TV','out_BV']], how='inner', on='Name')
    comparison = pd.merge(official_df[['Nr', 'Name', 'ZV_o', 'QV_o', 'TV_o', 'BV_o']], abs_routing_df[['Name', 'ZV', 'QV', 'TV', 'BV']], how='inner', on='Name')
    # comparison = pd.merge(official_df[['Nr.','Name','ZV','QV','TV','BV','Leer']], abs_routing_df[['Name','ZV','QV','TV','BV']], how='inner', on='Name')

    comparison = comparison.fillna(0)


    def sum_station_o(zv, qv, tv, bv, leer):
        #     sum_o = zv+qv+tv+bv+leer
        sum_o = zv + qv + tv + bv
        return sum_o


    def sum_station(zv, qv, tv, bv):
        sum_ = zv + qv + tv + bv
        return sum_


    # comparison['SUM_o'] = comparison.apply(lambda row: sum_station_o(int(row['ZV_o']),int(row['QV_o']),int(row['TV_o']),int(row['BV_o']),int(row['Leer'])), axis=1)
    comparison['SUM_o'] = comparison.apply(
        lambda row: sum_station(int(row['ZV_o']), int(row['QV_o']), int(row['TV_o']), int(row['BV_o'])), axis=1)

    comparison['SUM'] = comparison.apply(
        lambda row: sum_station(int(row['ZV']), int(row['QV']), int(row['TV']), int(row['BV'])), axis=1)
    # comparison['SUM'] = comparison.apply(lambda row: sum_station(int(row['ZV']),int(row['QV']),int(row['in_TV']),int(row['in_BV'])), axis=1)

    # comparison.to_csv(str(out_dir) + "\comparison.csv", sep = ",", index = None, encoding='latin1')
    comparison = comparison.astype({"Nr": int})
    comparison.to_csv(str(out_dir) + "/comparison_real_eu.csv", sep=",", index=None, encoding='latin1')




# MERGE NETWORKS FILES
# in_path = r"C:\Users\Ion\IVT\OSM_python"
# # in_path = r"C:\Users\gaion\freight\OSM_python"
# # in_path = r"C:\Users\gaion\Documents\Freight\OSM_python"
#
# # check output directory
# attempt = "eu123_ch4/2_routing"
# out_dir = str(attempt) + "/comparison_bygroups"
# network_path = str(in_path) + "/freight_data/" + str(attempt)
#
# if not os.path.exists(str(out_dir)):
#     os.makedirs(str(out_dir))
#     print('Directory created')
# else:
#     print('Directory exists, change name of attempt output directory')


# # LOAD routing results
# # abs_routing_df = pd.read_csv(str(network_path) + "\\abs_routing_df.csv",encoding='latin1')
# # abs_routing_df = pd.read_csv(str(network_path) + "\eu_routing_df.csv",encoding='latin1')
# abs_routing_df = pd.read_csv(str(out_dir) + "/groups.csv", encoding='latin1')
# # abs_routing_df = pd.read_csv(str(network_path) + "\\real_routing_df.csv",encoding='latin1')
# print('Nbc in abs_routing_df: '+ str(len(abs_routing_df)))
# # abs_routing_df.head()
#
# # LOAD official routing data
# # official_df = pd.read_csv(r"C:\Users\Ion\IVT\OSM_python\freight_data\freight\official_counting_o.csv")
# # official_df = pd.read_csv(str(network_path) + "\\real_routing_df.csv", encoding='latin1')
# official_df = pd.read_csv(str(out_dir) + "/groups_o.csv", encoding='latin1')
# # official_df = official_df.rename(columns={"ZV_o": "ZV", "QV_o": "QV", "TV_o": "TV", "BV_o": "BV"})
# # official_df.columns = ['Nr.','Name','ZV_o','QV_o','TV_o','in_TV','out_TV', 'BV_o','in_BV','out_BV']
# print('Nbc in official_df: '+ str(len(official_df)))
# official_df.head()


# DROP ROWS with no data
droprows=[]
threshold = 0
for i in range(0,len(official_df)):
    if (
    (math.isnan(official_df.iloc[i]['ZV_o']) == True or official_df.iloc[i]['ZV_o'] <threshold) and
    (math.isnan(official_df.iloc[i]['QV_o']) == True or official_df.iloc[i]['QV_o'] <threshold) and
    (math.isnan(official_df.iloc[i]['TV_o']) == True or official_df.iloc[i]['TV_o'] <threshold) and
    (math.isnan(official_df.iloc[i]['BV_o']) == True or official_df.iloc[i]['BV_o'] <threshold)
    ):
        droprows.append(i)
official_df=official_df.drop(official_df.index[droprows])
print(len(official_df))
official_df.head()

# comparison = pd.merge(official_df[['Nr.','Name','ZV_o','QV_o','TV_o','BV_o']], abs_routing_df[['Name','ZV','QV','in_TV','out_BV']], how='inner', on='Name')
# comparison = pd.merge(official_df[['Nr.','Name','ZV_o','QV_o','TV_o','BV_o', 'Leer']], abs_routing_df[['Name','ZV','QV','in_TV','in_BV']], how='inner', on='Name')
# comparison = pd.merge(official_df[['Nr.','Name','ZV_o','QV_o','TV_o','BV_o']], abs_routing_df[['Name','ZV','QV','out_TV','out_BV']], how='inner', on='Name')
comparison = pd.merge(official_df[['Nr', 'Name', 'ZV_o', 'QV_o', 'TV_o', 'BV_o']],
                      abs_routing_df[['Name', 'ZV', 'QV', 'TV', 'BV']], how='inner', on='Name')
# comparison = pd.merge(official_df[['Nr.','Name','ZV','QV','TV','BV','Leer']], abs_routing_df[['Name','ZV','QV','TV','BV']], how='inner', on='Name')

comparison = comparison.fillna(0)


def sum_station_o(zv, qv, tv, bv, leer):
    #     sum_o = zv+qv+tv+bv+leer
    sum_o = zv + qv + tv + bv
    return sum_o


def sum_station(zv, qv, tv, bv):
    sum_ = zv + qv + tv + bv
    return sum_


# comparison['SUM_o'] = comparison.apply(lambda row: sum_station_o(int(row['ZV_o']),int(row['QV_o']),int(row['TV_o']),int(row['BV_o']),int(row['Leer'])), axis=1)
comparison['SUM_o'] = comparison.apply(
    lambda row: sum_station(int(row['ZV_o']), int(row['QV_o']), int(row['TV_o']), int(row['BV_o'])), axis=1)

comparison['SUM'] = comparison.apply(
    lambda row: sum_station(int(row['ZV']), int(row['QV']), int(row['TV']), int(row['BV'])), axis=1)
# comparison['SUM'] = comparison.apply(lambda row: sum_station(int(row['ZV']),int(row['QV']),int(row['in_TV']),int(row['in_BV'])), axis=1)

# comparison.to_csv(str(out_dir) + "\comparison.csv", sep = ",", index = None, encoding='latin1')
comparison = comparison.astype({"Nr": int})
comparison.to_csv(str(out_dir) + "/comparison_real_eu.csv", sep=",", index=None, encoding='latin1')

# count_o = comparison.SUM_o.sum()
# count = comparison.SUM.sum()

# if count_o > count:
#     print('True: count_o > count ')
# else:
#     print('False')

# print('Original data counting: '+str(count_o))
# print('Predicted count: '+str('{:.4f}'.format(count)))
comparison

# -------------------------------------------------------------
# 1 - APPROACH COMPARE AMOUNT OF COUNT original/predicted
# -------------------------------------------------------------
# official_df2 = pd.read_csv(str(in_path) + "\\freight_data\\freight\\official_counting.csv")
# columns_o = ['ZV','QV','TV','BV','Leer']

official_df2 = official_df
columns_o = ['ZV_o', 'QV_o', 'TV_o', 'BV_o']

# columns = ['ZV','QV','in_TV','in_BV']
# columns = ['ZV','QV','out_TV','out_BV']
columns = ['ZV', 'QV', 'TV', 'BV']

count_o = 0
count = 0
leer_count = 0

# count predicted data
for i in range(0, len(comparison)):
    for k in range(0, len(columns)):
        column = columns[k]
        value = float(comparison.iloc[i][column])
        count += value

# count original data
for i in range(0, len(official_df2)):
    for j in range(0, len(columns_o)):
        column = columns_o[j]
        value_o = float(official_df2.iloc[i][column])
        if math.isnan(value_o) == True:
            value_o = 0
        if column == 'Leer':
            leer_count += value_o
        count_o += value_o

if count_o > count:
    print('True: count_o > count ')
else:
    print('False')

print('Original data counting: ' + str(count_o))
print('Predicted count: ' + str('{:.4f}'.format(count)))

print('Leer counting: ' + str(leer_count))
print('Difference: ' + str(count_o - count))


od_eu_df2 = pd.read_csv(str(network_path)+ '\od_europesum_df2.csv',sep = ",",low_memory=False,encoding='latin1')
def comp(weight,divisor,in1,out1,in2,out2,i):
    freq_o = weight#/divisor
#     if in1 != ' ' and out1 != ' ':
#         freq_o = weight
    return freq_o
od_eu_df2['suma'] = od_eu_df2.apply(lambda row: comp(row['WEIGHTING_FACTOR'],row['DIVISOR'],
                                                     row['BORDER_CROSSING_IN'],row['BORDER_CROSSING_OUT'],
                                                     row['in'],row['out'],
                                                     row.name),axis=1)
print(od_eu_df2.suma.sum())
print(len(od_eu_df2))
od_eu_df2.head()

od_eu_df2 = pd.read_csv(str(network_path) + '/od_europesum_df2.csv', sep=",", low_memory=False, encoding='latin1')
# od_ch_df2 = pd.read_csv(r'C:\Users\Ion\IVT\OSM_python\freight_data\eu123_ch4\od_chsum_df2.csv',sep = ",",low_memory=False,encoding='latin1')
# print(str(len(od_eu_df2)) + str(' ')+ str(len(od_ch_df2)))

sum_eu = 0
sum_ch = 0
for i in range(0, len(od_eu_df2)):
    freight_freq = od_eu_df2.iloc[i]['WEIGHTING_FACTOR']
    divisor = od_eu_df2.iloc[i]['DIVISOR']
    value = freight_freq / divisor
    sum_eu = sum_eu + value

# for i in range(0,len(od_ch_df2)):
#     value = od_ch_df2.iloc[i]['grossingFactor']
#     sum_ch = sum_ch + value

print(str('eu: ') + str(sum_eu))
# print(str('ch: ') + str(sum_ch))
# print(str('SUM: ') + str(sum_eu+sum_ch))


# -------------------------------------------------------------
# 2 - APPROACH CREATE ACC AND PRECISION DISTRIBUTED TABLE
# -------------------------------------------------------------
acc0 = [0, 0, 0, 0]
precision_list = []


def accuracy(official_value, predicted_value):
    if official_value != 0 and predicted_value != 0:
        precision_val = abs((official_value - predicted_value) / official_value)
        precision_list.append(precision_val)
        acc = abs(predicted_value / official_value)
    elif official_value == 0 and predicted_value == 0:
        precision_val = 0
        precision_list.append(precision_val)
        acc = 1
    else:
        #         try:
        #             precision_val = abs((official_value-predicted_value)/official_value)
        #         except:
        #             precision_val = abs((predicted_value-official_value)/predicted_value)

        #         precision_list.append(precision_val)
        if (official_value <= 1000 and official_value > 0) or (predicted_value <= 1000 and predicted_value > 0):
            acc = 1
            acc0[0] += 1
        elif (official_value > 1000 and official_value <= 7500) or (
                predicted_value > 1000 and predicted_value <= 75000):
            acc = 2
            acc0[1] += 1
        elif (official_value > 7500 and official_value <= 30000) or (
                predicted_value > 7500 and predicted_value <= 30000):
            acc = 5
            acc0[2] += 1
        elif official_value > 30000 or predicted_value > 30000:
            acc = 15
            acc0[3] += 1
    #     print(acc)
    return '{:.2f}'.format(acc)


comparison['ZV_acc'] = comparison.apply(lambda row: accuracy(row['ZV_o'], row['ZV']), axis=1)
comparison['QV_acc'] = comparison.apply(lambda row: accuracy(row['QV_o'], row['QV']), axis=1)
comparison['TV_acc'] = comparison.apply(lambda row: accuracy(row['TV_o'], row['TV'], ), axis=1)
comparison['BV_acc'] = comparison.apply(lambda row: accuracy(row['BV_o'], row['BV']), axis=1)

# comparison
acc_table = comparison[['Name', 'ZV_acc', 'QV_acc', 'TV_acc', 'BV_acc']]
acc_table.to_csv(str(out_dir) + "/acc_table.csv", sep=",", index=None, encoding='latin1')

# Create distribution of the accuracy
columns = ['ZV_acc', 'QV_acc', 'TV_acc', 'BV_acc']
acc = [0, 0, 0, 0, 0]
for i in range(0, len(acc_table)):
    for j in range(0, len(columns)):
        column = columns[j]
        value = float(acc_table.iloc[i][column])
        if value <= 1.4 and value >= 0.7:
            acc[0] += 1
        elif (value > 1.4 and value <= 2.5) or (value < 0.7 and value >= 0.4):
            acc[1] += 1
        elif (value > 2.5 and value <= 10) or (value < 0.4 and value >= 0.1):
            acc[2] += 1
        elif value > 10 or (value < 0.1 and value > 0):
            acc[3] += 1
        elif value == 0:
            acc[4] += 1

acc_p = ['{:.3f}'.format(x / sum(acc)) for x in acc]
precision = sum(precision_list) / len(precision_list)
print(sum(acc))
print('acc: ' + str(acc))
print('acc0: ' + str(acc0))
print('acc_p: ' + str(acc_p))
print('precision: ' + str(precision))

acc_table.head()



precision_list.sort(reverse=True)
highest_precision = []
for i in range (0,10):
    net = precision_list[i]
    highest_precision.append(net)
# print(str(max(precision_list)) + str())
print(highest_precision)
print(len(precision_list))
print('precision: ' + str(precision))
preci_list2 =[]
# for i in precision_list:
#     if i<4:
#         preci_list2.append(i)
# precision_list = preci_list2
print(len(precision_list))
precision = sum(precision_list)/len(precision_list)
print('precision: ' + str(precision))

high_prec = 0
threshold = 20
print(len(precision_list))
for i in precision_list:
    if i<(threshold/100):
        high_prec +=1
print('Acc lower than '+str(threshold) + '%: ' +str((high_prec*100)/len(precision_list)))

# -------------------------------------------------------------
# 3 - APPROACH: Compare original border crossing with chosen one in routing
# -------------------------------------------------------------
# create dictinoary with Nr and border crossing point
bc_nr = {}
for i in range(0, len(official_df2)):
    try:
        nr = int(official_df2.iloc[i]['Nr.'])
        name = official_df2.iloc[i]['Name']
        bc_nr[str(nr)] = name
    except:
        bc_nr[''] = 0
bc_nr[float('nan')] = 0
bc_nr['3070'] = 0  # not defined border crossing (the only one)
bc_nr[''] = 0
bc_nr_inv = {v: k for k, v in bc_nr.items()}

# Delete rows where the crossing borders match
od_incorrect = pd.read_csv(str(network_path) + '\od_europesum_df2.csv', sep=",", low_memory=False, encoding='latin1')
not_defined = []
droprows = []
diff_dict = {}
bc_0 = 0
bc_in = 0
bc_out = 0
bc_2 = 0
bc_2in = 0
bc_2out = 0


def match_bc(i, inout_bc, inout):
    real_id = od_incorrect.iloc[i][inout_bc].rstrip()
    #     try:
    real_bc = bc_id[real_id]
    pred_bc = od_incorrect.iloc[i][inout]
    if isinstance(pred_bc, str) == False and math.isnan(pred_bc) == True:
        pred_bc = 0
    if real_bc == pred_bc:
        return True
    else:
        return False


def is_in_dict(nr, in_out):
    if nr not in list(diff_dict) and nr != ' ':
        diff_dict[nr] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        diff_dict[nr][in_out] = 1
    elif nr != ' ':
        diff_dict[nr][in_out] += 1


for i in range(0, len(od_incorrect)):
    in_comp = match_bc(i, 'BORDER_CROSSING_IN', 'in')
    out_comp = match_bc(i, 'BORDER_CROSSING_OUT', 'out')
    in_bc = od_incorrect.iloc[i]['BORDER_CROSSING_IN']
    out_bc = od_incorrect.iloc[i]['BORDER_CROSSING_OUT']
    try:
        in_p = bc_id_inv[od_incorrect.iloc[i]['in']]
    except:
        in_p = 0
    try:
        out_p = bc_id_inv[od_incorrect.iloc[i]['out']]
    except:
        out_p = 0

    if in_comp == True and out_comp == True:
        droprows.append(i)
        is_in_dict(in_bc, 4)
        is_in_dict(out_bc, 5)
    #         bc_inc += 1
    else:
        # Distribution of the incorrect bc by trip type
        if in_bc != ' ' and out_bc != ' ':
            if in_comp == False and out_comp == False:
                bc_2 += 1
                is_in_dict(in_bc, 0)
                is_in_dict(out_bc, 1)
                is_in_dict(in_p, 7)
                is_in_dict(out_p, 8)
            elif in_comp == True and out_comp == False:
                bc_2out += 1
                is_in_dict(out_bc, 1)
                is_in_dict(out_p, 8)
            elif in_comp == False and out_comp == True:
                bc_2in += 1
                is_in_dict(in_bc, 0)
                is_in_dict(in_p, 7)
        elif in_bc != ' ' and out_bc == ' ':
            bc_in += 1
            is_in_dict(in_bc, 0)
            is_in_dict(in_p, 7)
        elif in_bc == ' ' and out_bc != ' ':
            bc_out += 1
            is_in_dict(out_bc, 1)
            is_in_dict(out_p, 8)
        else:
            bc_0 += 1
    print(i, end="\r")

# drop well predicted lines from dataframe
od_incorrect = od_incorrect.drop(od_incorrect.index[droprows])
# del diff_dict[0]
# add SUM and Name to the dictionary
for i in range(0, len(list(diff_dict))):
    nr = list(diff_dict)[i]
    diff_dict[nr][2] = diff_dict[nr][0] + diff_dict[nr][1]
    diff_dict[nr][6] = diff_dict[nr][4] + diff_dict[nr][5]
    diff_dict[nr][9] = diff_dict[nr][7] + diff_dict[nr][8]
    diff_dict[nr][3] = bc_id[nr]
# create dataframes and export to csv files
diff_df = pd.DataFrame(
    columns=['Nr.', 'Name', 'missed_IN', 'missed_OUT', 'missed_SUM', 'correct_pIN', 'correct_pOUT', 'correct_pSUM',
             'wrong_pIN', 'wrong_pOUT', 'wrong_pSUM'])
j = 0
for i in list(diff_dict):
    diff_df.loc[j] = [i,
                      diff_dict[i][3],
                      diff_dict[i][0],
                      diff_dict[i][1],
                      diff_dict[i][2],
                      diff_dict[i][4],
                      diff_dict[i][5],
                      diff_dict[i][6],
                      diff_dict[i][7],
                      diff_dict[i][8],
                      diff_dict[i][9]
                      ]
    j += 1
diff_df = diff_df.sort_values(by=['missed_SUM', 'missed_IN', 'missed_OUT'], ascending=False)
od_incorrect.to_csv(str(out_dir) + '\od_incorrect.csv', sep=",", index=None, encoding='latin1')
diff_df.to_csv(str(out_dir) + '\diff_df.csv', sep=",", index=None, encoding='latin1')

print(str(len(od_incorrect)) + str(' ') + str(len(od_eu_df2)))

print(str('0 Bc: ') + str(bc_0))
print(str('1 Bc_in: ') + str(bc_in))
print(str('1 Bc_out: ') + str(bc_out))
print(str('2 Bc: ') + str(bc_2))
print(str('2 Bc_in: ') + str(bc_2in))
print(str('2 Bc_out: ') + str(bc_2out))

print(len(diff_df))
diff_df.head()



# create dictinoary with Nr and border crossing point
official_df3 = pd.read_csv(str(in_path) + "/freight_data/freight/official_counting.csv")

bc_id_o = {}
for i in range(0,len(official_df3)):
    nr = official_df3.iloc[i]['Nr.']
    name = official_df3.iloc[i]['Name']
    bc_id_o[str(nr)] = name
bc_id_o[''] = ''
bc_id_o['3070'] = ''
# bc_id_o[float('nan')] = 0
bc_id_o_inv = {v: k for k, v in bc_id_o.items()}
bc_id_o_inv[0] = ''
bc_id_o_inv[''] = ''

# del bc_id_o_inv['']
for i in range(1,12):
#     del bc_nr[str(i)]
    for nr in bc_id[str(i)][1]:
        name = bc_id_o[str(nr)]
        bc_id_o_inv[str(name)] = str(i)#bc_id_o[str(nr)]

bc_nr = {}

for i in range(0, len(official_df2)):
    try:
        nr = int(official_df2.iloc[i]['Nr'])
        name = official_df2.iloc[i]['Name']
        bc_nr[str(nr)] = name
    except:
        bc_nr[''] = ''

for i in range(1, 12):
    del bc_nr[str(i)]
    for nr in bc_id[str(i)][1]:
        bc_nr[str(nr)] = str(i)  # bc_id_o[str(nr)]
bc_nr[float('nan')] = 0
bc_nr['3070'] = 0  # not defined border crossing (the only one)
# bc_nr[''] = ''
# bc_nr_inv = {v: k for k, v in bc_nr.items()}


i = 9475
[in_comp,in_bc,in_p] = match_bc(i,'BORDER_CROSSING_IN','in')
[out_comp,out_bc,out_p] = match_bc(i,'BORDER_CROSSING_OUT','out')
print(in_comp,in_bc,in_p)
print(out_comp,out_bc,out_p)


def match_bc(i, inout_bc, inout):
    nr = od_incorrect.iloc[i][inout_bc].rstrip()  # this gives a nr
    #     real_bc = bc_id[nr] #this gives a name, gives 0 for nr = ''
    #     real_id =bc_nr[nr]
    name = bc_id_o[nr]  # this gives a name, gives 0 for nr = ''
    real_id = bc_id_o_inv[name]

    #     real_id = find_group(str(nr)) #this gives a number from 0 to 11

    pred_bc = od_incorrect.iloc[i][inout]  # this gives a name
    if isinstance(pred_bc, str) == False and math.isnan(pred_bc) == True:
        pred_bc = 0
    pred_id = bc_id_o_inv[pred_bc]  # this gives a nr, '' in case of pred_bc = 0
    #     pred_id = find_group(pred_bc) #this gives a number from 0 to 11, 0 in case of pred_id = '' OR the actual nr

    if real_id == pred_id:
        match = True
        count.append(i)
    #         print (i, end="\r")
    else:
        match = False
    #     print(match,real_id,pred_id)
    return match, real_id, pred_id


# -------------------------------------------------------------
# 3 - APPROACH: Compare original border crossing with chosen one in routing
# -------------------------------------------------------------
# IMPORTAR DICCIONARIO CON LOS GRUPOS Y CADA NR EN CADA GRUPO Y ASOCIARLOS PARA PODER HACER LA ULTIMA TABLE
file = open(str(in_path) + "/freight_data/freight/bc_id.pkl", 'rb')
bc_id = pickle.load(file)
file.close()
print('Nnr in bc_id: ' + str(len(bc_id)))

# Delete rows where the crossing borders match
od_incorrect = pd.read_csv(str(network_path) + '/od_europesum_df2.csv', sep=",", low_memory=False, encoding='latin1')
not_defined = []
droprows = []
diff_dict = {}
bc_0 = 0
bc_in = 0
bc_out = 0
bc_2 = 0
bc_2in = 0
bc_2out = 0
count = []


# def find_group(nr):
#     for i in range(1,12):
#         if nr in bc_id[str(i)][1]:
#             final_id = str(i)
#             return final_id
#     final_id = str(nr)
#     return final_id #could be a group (from)

def match_bc(i, inout_bc, inout):
    nr = od_incorrect.iloc[i][inout_bc].rstrip()  # this gives a nr
    #     real_bc = bc_id[nr] #this gives a name, gives 0 for nr = ''
    #     real_id =bc_nr[nr]
    name = bc_id_o[nr]  # this gives a name, gives 0 for nr = ''
    real_id = bc_id_o_inv[name]
    #     real_id = find_group(str(nr)) #this gives a number from 0 to 11

    pred_bc = od_incorrect.iloc[i][inout]  # this gives a name
    if isinstance(pred_bc, str) == False and math.isnan(pred_bc) == True:
        pred_bc = 0
    pred_id = bc_id_o_inv[pred_bc]  # this gives a nr, '' in case of pred_bc = 0
    #     pred_id = find_group(pred_bc) #this gives a number from 0 to 11, 0 in case of pred_id = '' OR the actual nr

    if real_id == pred_id:
        match = True
    else:
        match = False
    return match, real_id, pred_id


def is_in_dict(nr, in_out):
    if nr not in list(diff_dict) and nr != ' ':
        diff_dict[nr] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        diff_dict[nr][in_out] = 1
    elif nr != ' ':
        diff_dict[nr][in_out] += 1


for i in range(0, len(od_incorrect)):
    [in_comp, in_bc, in_p] = match_bc(i, 'BORDER_CROSSING_IN', 'in')
    [out_comp, out_bc, out_p] = match_bc(i, 'BORDER_CROSSING_OUT', 'out')

    if in_comp == True and out_comp == True:
        droprows.append(i)
        is_in_dict(in_bc, 4)
        is_in_dict(out_bc, 5)
    else:
        # Distribution of the incorrect bc by trip type
        if in_bc != '' and out_bc != '':
            if in_comp == False and out_comp == False:
                bc_2 += 1
                is_in_dict(in_bc, 0)
                is_in_dict(out_bc, 1)
                is_in_dict(in_p, 7)
                is_in_dict(out_p, 8)
            elif in_comp == True and out_comp == False:
                bc_2out += 1
                is_in_dict(out_bc, 1)
                is_in_dict(out_p, 8)
            elif in_comp == False and out_comp == True:
                bc_2in += 1
                is_in_dict(in_bc, 0)
                is_in_dict(in_p, 7)
        elif in_bc != '' and out_bc == '':
            bc_in += 1
            is_in_dict(in_bc, 0)
            is_in_dict(in_p, 7)
        elif in_bc == '' and out_bc != '':
            bc_out += 1
            is_in_dict(out_bc, 1)
            is_in_dict(out_p, 8)
        else:
            bc_0 += 1
    # print(i, end="\r")

# drop well predicted lines from dataframe
od_incorrect = od_incorrect.drop(od_incorrect.index[droprows])

# add SUM and Name to the dictionary
for i in range(0, len(list(diff_dict))):
    nr = list(diff_dict)[i]
    diff_dict[nr][2] = diff_dict[nr][0] + diff_dict[nr][1]
    diff_dict[nr][6] = diff_dict[nr][4] + diff_dict[nr][5]
    diff_dict[nr][9] = diff_dict[nr][7] + diff_dict[nr][8]
    try:
        diff_dict[nr][3] = bc_id[nr][0]
    except:
        diff_dict[nr][3] = ''
# create dataframes and export to csv files
diff_df = pd.DataFrame(
    columns=['Nr.', 'Name', 'missed_IN', 'missed_OUT', 'missed_SUM', 'correct_pIN', 'correct_pOUT', 'correct_pSUM',
             'wrong_pIN', 'wrong_pOUT', 'wrong_pSUM'])
j = 0
for i in list(diff_dict):
    diff_df.loc[j] = [i,
                      diff_dict[i][3],
                      diff_dict[i][0],
                      diff_dict[i][1],
                      diff_dict[i][2],
                      diff_dict[i][4],
                      diff_dict[i][5],
                      diff_dict[i][6],
                      diff_dict[i][7],
                      diff_dict[i][8],
                      diff_dict[i][9]
                      ]
    j += 1
diff_df = diff_df.sort_values(by=['missed_SUM', 'missed_IN', 'missed_OUT'], ascending=False)
od_incorrect.to_csv(str(out_dir) + '\od_incorrect.csv', sep=",", index=None, encoding='latin1')
diff_df.to_csv(str(out_dir) + '\diff_df.csv', sep=",", index=None, encoding='latin1')

print(str(len(od_incorrect)) + str(' ') + str(len(od_eu_df2)))

print(str('0 Bc: ') + str(bc_0))
print(str('1 Bc_in: ') + str(bc_in))
print(str('1 Bc_out: ') + str(bc_out))
print(str('2 Bc: ') + str(bc_2))
print(str('2 Bc_in: ') + str(bc_2in))
print(str('2 Bc_out: ') + str(bc_2out))

print(len(diff_df))
diff_df.head()

# CHECK IF AMOUNT OF BORDER CROSSINGS IS FINE in od_incorrect
print(len(od_incorrect))
droprows = []
for i in range(0, len(od_incorrect)):
    check_bc = [0, 0, 0, 0]

    real_in = od_incorrect.iloc[i]['BORDER_CROSSING_IN']
    real_out = od_incorrect.iloc[i]['BORDER_CROSSING_OUT']
    if real_in != ' ' and len(real_in) == 4:
        check_bc[0] = 1
    if real_out != ' ' and len(real_out) == 4:
        check_bc[1] = 1

    try:
        pred_in = math.isnan(od_incorrect.iloc[i]['in'])
    except:
        check_bc[2] = 1
    try:
        pred_out = math.isnan(od_incorrect.iloc[i]['out'])
    except:
        check_bc[3] = 1

    if check_bc[0] == check_bc[2] and check_bc[1] == check_bc[3]:
        droprows.append(i)
    print(i, end="\r")

od_incorrect = od_incorrect.drop(od_incorrect.index[droprows])
od_incorrect.to_csv(str(out_dir) + "/od_incorrect_DABC.csv", sep=",", index=None, encoding='latin1')

print(len(od_incorrect))