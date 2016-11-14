#!/usr/bin/python
#-*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os
import datetime
# create some global variable like: zone_dict
zone_dict = {}
zone_list = []


# tool function
def time2slot(time):
	hms = time.split(':')
	hour = hms[0]
	minute = hms[1]
	second = hms[2]
	cur = int(hour) * 6 + int(minute) / 10 + 1
	return cur

def traffic2flow(traffic):
	pair = traffic.split(':')
	level = pair[0]
	flow = pair[1]
	return int(flow)

# cluster_map

def createZoneDict(filepath):
	zonefile = file(filepath,'r')
	for line in zonefile:
		column = line.split('\n')[0]
		columns = column.split('\t')
		if columns:
			zone_dict[columns[0]] = columns[1]
			zone_list.append(columns[0])

# generate basic full table
def generateFullTable(gridnum, slotnum):
    g = range(1,gridnum+1)
    s = range(1,slotnum+1)
    gridDF = pd.DataFrame(data = g, columns = ['grid'])
    slotDF = pd.DataFrame(data = s, columns = ['slot'])
    new_df = pd.DataFrame(columns=['grid','slot'])

    for w_index,w_row in gridDF.iterrows():
        for d_index,d_row in slotDF.iterrows():
            g_data = w_row['grid']
            s_data = d_row['slot']

            row =  pd.DataFrame([dict(grid=g_data, slot=s_data), ])
            new_df = new_df.append(row, ignore_index=True)
    return new_df

# poi_table

def createPoiDF(filepath):
	poifile = file(filepath,'r')
	max_level1_number = 0
	max_level2_number = 0

	for line in poifile:
		col = line.split('\n')[0]
		pois = col.split('\t')

		i = 0
		for i in range(1, len(pois)):
			poi = pois[i].split(':')[0].split('#')	
			if(int(poi[0]) > max_level1_number):
				max_level1_number = int(poi[0])
			if(len(poi) > 1 and int(poi[1]) > max_level2_number):
				max_level2_number = int(poi[1])

	print "level1 poi number is "
	print max_level1_number
	print "level2 poi number is "
	print max_level2_number

	poi_matrix = [[0 for x in range(max_level1_number * max_level2_number)] for y in range(len(zone_list))]

	for line in poifile:
		col = line.split('\n')[0]
		pois = col.split('\t')
		row = int(zone_list[pois[0]])
		for i in range(1, len(pois)):
			poi = pois[i].split(':')
			levels = poi[0].split('#')
			if(len(levels) == 1):
				poi_matrix[row][(int(levels[0]) - 1) * max_level1_number] = int(poi[1])
			if(len(levels) > 1):
				poi_matrix[row][(int(levels[0]) - 1) * max_level1_number + int(levels[1])] = int(poi[1])

	poi = pd.DataFrame(data = poi_matrix, columns = range(0, 25*18), index = range(1, 67))
	poi['grid'] = poi.index
	return poi
	



# order 

def createOrderDF(filepath,filename):
	# origin df
	order = pd.read_table(filepath+filename, delim_whitespace=True, header=None)
	col = ['oid','did','pid','start','end','price','date','time']
	order.columns = col
	# some info
	print "order length of file  "+ filename +" is : "
	print order.size / 8
	driver = set(order['did'])
	print "distinct driver number: "
	print len(driver)
	passenger = set(order['pid'])
	print "distinct passenger number: "
	print len(passenger)
	# create new column grid
	order['grid'] = order['start'].map(lambda x : int(zone_dict[x]))
	# create new column slot
	order['slot'] = order['time'].map(lambda x: time2slot(x))
	return order

def createAcceptOrder(totalOrder):
	accept = totalOrder[totalOrder['did'].notnull()]
	return accept

def createGapOrder(totalOrder):
	gap = totalOrder[totalOrder['did'].isnull()]
	return gap


# weather 
def createWeatherDF(filepath, filename):
	weather = pd.read_table(filepath + filename, delim_whitespace = True, header = None)
	wea_col = ['date', 'time', 'type', 'temperature', 'pm25']
	weather.columns = wea_col
	weather['slot'] = weather['time'].map(lambda x : time2slot(x))
	return weather 

# traffic

def createTrafficDF(filepath, filename):
	traffic = pd.read_table(filepath+filename, delim_whitespace=True, header=None)
	tra_col = ['index','one','two','three','four','date','time']
	traffic.columns = tra_col
	traffic['slot'] = traffic['time'].map(lambda x: time2slot(x))
	traffic['grid'] = traffic['index'].map(lambda x: int(zone_dict[x]))
	traffic['one'] = traffic['one'].map(lambda x: traffic2flow(x))
	traffic['two'] = traffic['two'].map(lambda x: traffic2flow(x))
	traffic['three'] = traffic['three'].map(lambda x: traffic2flow(x))
	traffic['four'] = traffic['four'].map(lambda x: traffic2flow(x))
	return traffic

def createTrainingSet(order, gap, poi, weather, traffic):
	full = generateFullTable(66, 144)
	# past three time slot's order count, gap count, and current gap count is the target

	# request
	order = order.fillna(0)
	basic = order.groupby(['grid','slot'], as_index = False)
	avg_price = basic['price'].agg({'avg_price':np.mean})
	slot_count = basic['time'].agg({'count':np.count_nonzero})
	order_count = pd.merge(full, slot_count, how = 'left', on = ['grid', 'slot'])
	order_count_price = pd.merge(order_count, avg_price, how = 'left', on = ['grid', 'slot'])
	order_count_price.fillna(0, inplace=True)

	# gap
	gap = gap.fillna(0)
	gap_basic = gap.groupby(['grid','slot'], as_index = False)
	# avg_price = basic['price'].agg({'avg_price':np.mean}) i guess all is null for a gap record right?
	gap_slot_count = gap_basic['time'].agg({'gap_count':np.count_nonzero})
	gap_count = pd.merge(full, gap_slot_count, how = 'left', on = ['grid', 'slot'])
	gap_count.fillna(0, inplace=True)
	# order  /// no need to group by date because we treate each file seperately, only concatrate them in the end
	#        /// idealy, we get 66 * (144 - 2) * 21 rows..
	#        /// each row we consider its past three time slot, what's the avg req count, avg accept price, avg gap count
	#        /// and what's the current gap.. so i guess we have to do the statistic on the whole set .. not only gap...

	orderinfo = order_count_price
	orderinfo['gap_count'] = gap_count['gap_count']
	#orderinfo = pd.merge(avg_price, count, how = 'outer', on = ['grid','slot'])
	orderinfo['avg_price_3'] = pd.rolling_mean(orderinfo['avg_price'],3).shift(1)
	orderinfo['avg_count_3'] = pd.rolling_mean(orderinfo['count'], 3).shift(1)
	orderinfo['avg_gap_count'] = pd.rolling_mean(orderinfo['gap_count'], 3).shift(1)

	orderinfo['price_delta_1'] = orderinfo['avg_price'] / orderinfo['avg_price'].shift(1) - 1
	orderinfo['price_delta_3'] = pd.rolling_mean(orderinfo['price_delta_1'],2).shift(1)

	orderinfo['count_delta_1'] = orderinfo['count'] / orderinfo['count'].shift(1) - 1
	orderinfo['count_delta_3'] = pd.rolling_mean(orderinfo['count_delta_1'],2).shift(1)

	orderinfo['gap_delta_1'] = orderinfo['gap_count'] / orderinfo['gap_count'].shift(1) - 1
	orderinfo['gap_delta_3'] = pd.rolling_mean(orderinfo['gap_delta_1'],2).shift(1)

	# o = orderinfo.dropna(how = 'any')
	orderinfo.fillna(0, inplace = True)
	del orderinfo['avg_price']
    #del o['count'] # gap_count predict target...
	del orderinfo['price_delta_1']
	del orderinfo['count_delta_1']
	del orderinfo['gap_delta_1']
	# slot grid avg_price_3 avg_count_3 price_delta_3 count_delta_3

	# weather
	wgb = weather.groupby(['slot'], as_index = False)
	wtype = wgb['type'].agg({'wtype':np.max})
	wtemp = wgb['temperature'].agg({'wtemp':np.mean})
	wpm25 = wgb['pm25'].agg({'wpm25':np.mean})
	wea1 = pd.merge(wtype, wtemp, how = 'outer', on = ['slot'])
	wea = pd.merge(wea1, wpm25, how = 'outer', on = ['slot'])
	wea['min_type_3'] = pd.rolling_min(wea['wtype'],3).shift(1)
	wea['avg_temp_3'] = pd.rolling_mean(wea['wtemp'], 3).shift(1)
	wea['avg_pm25_3'] = pd.rolling_mean(wea['wpm25'],3).shift(1)
	# w = wea.dropna(how = 'any')
	del wea['wtype']
	del wea['wtemp']
	del wea['wpm25']
	# + weather
	orderweather = pd.merge(orderinfo, wea, how = 'left', on = ['slot'])
	orderweather.fillna(0, inplace = True)
	# + traffic  to-do  inner to avoid null 

	
	traffic['avg_one_3'] = pd.rolling_mean(traffic['one'],3).shift(1)
	traffic['avg_two_3'] = pd.rolling_mean(traffic['two'], 3).shift(1)
	traffic['avg_three_3'] = pd.rolling_mean(traffic['three'],3).shift(1)
	traffic['avg_four_3'] = pd.rolling_mean(traffic['four'],3).shift(1)
	#t = traffic.dropna(how = 'any')
	del traffic['one']
	del traffic['two']
	del traffic['three']
	del traffic['four']

	orderweathertraffic = pd.merge(orderweather, traffic, how = 'left', on = ['slot','grid'])
	orderweathertraffic.fillna(method = 'backfill', inplace = True)
	# + poi
	owtp = pd.merge(orderweathertraffic, poi, how = 'left', on = ['grid'])
	
	del owtp['index']
	# del orderpoiweathertraffic['date']
	del owtp['time']
	#  orderpoiweathertraffic.fillna(orderpoiweathertraffic.mean())

	return owtp

def main():

	createZoneDict("./cluster_map/cluster_map")
	poi = createPoiDF("./poi_data/poi_data")
	training_dates = []
	
	date1 = '2016-01-01'
	date2 = '2016-01-21'
	start = datetime.datetime.strptime(date1, '%Y-%m-%d')
	end = datetime.datetime.strptime(date2, '%Y-%m-%d')
	step = datetime.timedelta(days=1)
	while start <= end:
		training_dates.append(str(start.date()))
		start += step
	# pd.read_csv(os.path.join(root,inputFile))
	frames = []
	for date in training_dates:
		order = createOrderDF("./order_data/order_data_", date)
		gap = createGapOrder(order)
		weather = createWeatherDF("./weather_data/weather_data_", date)
		traffic = createTrafficDF("./traffic_data/traffic_data_", date)
		#gap.write_csv("./order_data/gap_"+date)
		#poi.write_csv("./poi_data/poi_data_1")
		#weather.write_csv("./weather_data/weather_"+date)
		#traffic.write_csv("./traffic_data/traffic_"+date)
		#training = createTrainingSet(order, poi, weather, traffic)
		training = createTrainingSet(order, gap, poi, weather, traffic)
		frames.append(training)
	result = pd.concat(frames)
	result['dayOfWeek'] = pd.DatetimeIndex(result.date).dayofweek
	print result.columns
	print result.size / len(result.columns)
	print len(result.columns)
	print 66 * 144 * 21
	# the average data of former three time slot as feature and the current slot gap count as target
	result.to_csv("./gap_training_set_req_gap_features.csv", index=False)



if __name__ == '__main__':
	main()