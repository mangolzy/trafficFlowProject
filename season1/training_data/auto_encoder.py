#!/usr/bin/python
#-*- coding: utf-8 -*-


import pandas as pd
import numpy as np
import os
import datetime


# construct the auto-encoder input ..  66 * r  from farest to nearest slot supervised output 66 * 1
# assume that the length of samples is the same as constructed training_set, also 3->1 pairs
# output:  21 * 144    *    66
# input:   21 * 144    *    66 * r
# orderDF is the train_set file
# len: 21 * 144
def constructInput(orderDF, r, Len):
    input_matrix = [[0 for x in range(66*r)] for y in range(Len+2)]
    output_matrix = [[0 for x in range(66)] for y in range(Len)]
    date = 1
    for index, row in orderDF.iterrows():
        #print row['count']
        # count in current slot
        slot = int(row['slot'])
        grid = int(row['grid'])
        count =int(row['count'])
        #date = datetime.datetime.strptime(row['date'],"%Y-%m-%d").day
        if(grid == 66 and slot == 144):
            date = date + 1
        #print count
        output_matrix[(date - 1) * 144 + (slot-1)][grid-1] = count
        for i in range(r):
        	input_matrix[(date - 1) * 144 + slot + i][66 * (r-i) + grid-1] = count
        	#input_matrix[slot + 1][66 * 1 + grid-1] = count
        	#input_matrix[slot + 2][66 * 0 + grid-1] = count
    #to-do: you need to skip the first three slot every day... then we got a training set for the auto-encoder..
    #predict for every thirty minutes
    inputdf = pd.DataFrame(data = input_matrix, columns = range(0, 66*r), index = range(0,Len))
    output = pd.DataFrame(data = output_matrix, columns = range(0, 66), index = range(0,Len))

    print date
    return inputdf, output      

 def main():
 	order = pd.read_csv('./gap_training_set.csv')
 	feature, target = constructInput(order, 3, 21 * 144) 
 	# auto_encoder

 if __name__ == '__main__':
       	main()      