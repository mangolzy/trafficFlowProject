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

def createTrainingSet(order, poi, weather, traffic):
	basic = order.groupby(['grid','slot'], as_index = False)
	avg_price = basic['price'].agg({'avg_price':np.mean})
	count = basic['time'].agg({'count':np.count_nonzero})
	# order
	orderinfo = pd.merge(avg_price, count, how = 'outer', on = ['grid','slot'])
	orderinfo['avg_price_3'] = pd.rolling_mean(orderinfo['avg_price'],3).shift(1)
	orderinfo['avg_count_3'] = pd.rolling_mean(orderinfo['count'], 3).shift(1)
	orderinfo['price_delta_1'] = orderinfo['avg_price'] / orderinfo['avg_price'].shift(1) - 1
	orderinfo['price_delta_3'] = pd.rolling_mean(orderinfo['price_delta_1'],2).shift(1)
	orderinfo['count_delta_1'] = orderinfo['count'] / orderinfo['count'].shift(1) - 1
	orderinfo['count_delta_3'] = pd.rolling_mean(orderinfo['count_delta_1'],2).shift(1)
	o = orderinfo.dropna(how = 'any')
	del o['avg_price']
	# del o['count'] predict target...
	del o['price_delta_1']
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
	w = wea.dropna(how = 'any')
	del w['wtype']
	del w['wtemp']
	del w['wpm25']
	# + weather
	orderweather = pd.merge(o, w, how = 'left', on = ['slot'])

	# + traffic  to-do  inner to avoid null 

	
	traffic['avg_one_3'] = pd.rolling_mean(traffic['one'],3).shift(1)
	traffic['avg_two_3'] = pd.rolling_mean(traffic['two'], 3).shift(1)
	traffic['avg_three_3'] = pd.rolling_mean(traffic['three'],3).shift(1)
	traffic['avg_four_3'] = pd.rolling_mean(traffic['four'],3).shift(1)
	t = traffic.dropna(how = 'any')
	del t['one']
	del t['two']
	del t['three']
	del t['four']

	orderweathertraffic = pd.merge(orderweather, t, how = 'inner', on = ['slot','grid'])
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
		training = createTrainingSet(order, poi, weather, traffic)
		frames.append(training)
	result = pd.concat(frames)
	result['dayOfWeek'] = pd.DatetimeIndex(result.date).dayofweek
	print result.columns
	print result.size / len(result.columns)
	print len(result.columns)
	# the average data of former three time slot as feature and the current slot gap count as target
	result.to_csv("./gap_training_set.csv", index=False)



if __name__ == '__main__':
	main()