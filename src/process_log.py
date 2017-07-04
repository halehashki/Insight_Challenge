#!/usr/bin/env python

#######
##Author Haleh Ashki, 07/02/17
##Coding chalenge for Insight Data Engineering
######


import math
from collections import defaultdict
import operator
import sys
import os








##@input filename to open and read from, Purchase and netwoek dictionary, flag to indicate its that for anomaly purchase or not, degree and number of purchased, output file name
#@return  updated network and purchase dictionary, and D and T
#@brief This funstion get input file and based on flag do two things. If flad is 1, it reads the input file and update the newtwork and purchase dictionary based on input file event.
# if the flag is 2, meaning its streamfile, it calls another function to detect if this purchase is anomaly or not.
#flag= 1: readfile and update network and purchase dictionary. flag=2 call another function
def make_network_purchase_dictionary(filename,Dic_Network,Dic_purchase, flag, D, T,output_file):
    counter=0 ## for line number
    with open(filename) as f:
        for line in f:
                line=line.rstrip() ## detecting and ignoring emty lines in the input file
                if line:
                    if line[0] == "{" : ### check the input file format and report and exit if the line dont have the same format as otehr lines
                        counter=counter+1
                        linesplit=[x.strip() for x in line.split(',')]
                        linesplit2=linesplit[0].split(":")

                        ### extract the id, amount, date and time of the purchase
                        if linesplit2[1].replace('\"','') == "purchase":
                            id1=int(linesplit[2].split(":")[1].replace('\"',''))
                            amount=float(linesplit[3].split(":")[1].replace('\"','').replace('}',''))
                            time_val=linesplit[1].split(" ")[1].replace('\"','')
                            date_val=linesplit[1].split(" ")[0].split(":")[1].replace('\"','')

                            ### flag =2 call find_anomalous_purchase function and then update the network and purchase dictionaries
                            if flag == 1:
                                Dic_purchase[id1].append((date_val,time_val,amount))
                            if flag == 2:
                                find_anomalous_purchase(id1,amount,D,T,Dic_Network,Dic_purchase,date_val,time_val,output_file)
                                Dic_purchase[id1].append((date_val,time_val,amount))

                        ## update network dictionary by adding new friens
                        elif  linesplit2[1].replace('\"','') == "befriend":
                            id1=int(linesplit[2].split(":")[1].replace('\"',''))
                            id2=int(linesplit[3].split(":")[1].replace('\"','').replace('}',''))
                            Dic_Network[id1].append(id2)
                            Dic_Network[id2].append(id1)

                        ## update network dictionary by deleting the friendship
                        elif linesplit2[1].replace('\"','')== "unfriend":
                            id1=int(linesplit[2].split(":")[1].replace('\"',''))
                            id2=int(linesplit[3].split(":")[1].replace('\"','').replace('}',''))
                            Dic_Network[id1].remove(id2)
                            Dic_Network[id2].remove(id1)

                        ## if its batchfile, it extract D and T
                        elif linesplit2[0].replace('\"','').replace('{','') == "D":
                            D=linesplit2[1].replace('\"','')
                            T=linesplit[1].split(" ")[0].split(":")[1].replace('\"','').replace('}','')
                        else:
                            print "Error in input file at line ", counter+1
                            sys.exit()

                    else:
                        print "Error in input file at line ", counter+1
                        sys.exit()




    return Dic_Network,Dic_purchase,D,T


##@brief this function get the information of a purchase and detect if its amnomaly and reported it in a output file
##To eliminate using libreries, I used the python math for calculating math and std, I have hard coded using numpy which is faster, but its commented for now.
## that's an option when the data file is huge
## D=1 and T=2 are default ones
##Based on the D, it collect all nodes in the network and get all last T purchases. Then get the meand and std and detect if thats anomaly or not.
def find_anomalous_purchase(id1,amount,D,T,Dic_Network,Dic_purchase, date_val,time_val, output_file ):
    nodelist=Dic_Network[id1]
    if D>1 :
        nodelistnew=[]
        for i in range(1,D):
            for i in nodelist:
                for j in Dic_Network[i]:
                    nodelistnew.append(j)

                nodelist=set(nodelistnew)  ## remove redundancy and duplicate nodes


    if len(nodelist) < 1 :
        print "The customer with id ", id1, " has no friend in his/her network"
        sys.exit()

    all_amounts=[]
    for i in nodelist:  ### get all last T purchases amonut for each eligible node in the network
        sorted1=sorted(Dic_purchase[i], key=lambda x: x[0])
        eligible=sorted1[0:T+1]

        all_temp=([x[2] for x in eligible])
        for j in all_temp:
            all_amounts.append(j)

    if len(all_amounts) < 1:
        print " there is no previuos purchase in network of customer id", id1sys.exit()
    #### using numpy for calculating mean and std
    #mean_val=np.mean(all_amounts)
    #mean_val=float("{0:.2f}".format(mean_val))
    #std_val=np.std(all_amounts)
    #std_val=float("{0:.2f}".format(std_val))

    ##### using python math library for calculating mean and std
    num_items = len(all_amounts)
    mean_val = sum(all_amounts) / num_items
    mean_val=float("{0:.2f}".format(mean_val))
    differences = [x - mean_val for x in all_amounts]
    sq_differences = [d ** 2 for d in differences]
    ssd = math.sqrt(sum(sq_differences)/ num_items)
    std_val=float("{0:.2f}".format(ssd))



    if amount > (mean_val + (3 * std_val)) :

            strout='{"event_type":"purchase", "timestamp":"' + str(date_val) + ' ' +str(time_val) + '", "id":"' + str(id1) + '", "amount": "' + str(amount) + '", "mean": "' + str(mean_val) + '", "sd": "' + str(std_val) + '"}\n'
            #print strout
            output_file.write(strout)



#### the input files and outfile name are given as argument in terminal
#### checking if the format is correct
if  len(sys.argv) >4 :
    print "There are more than expected given arguments"
    sys.exit()
elif  len(sys.argv) <4 :
    print "There are less than expected given arguments"
    sys.exit()


### extract given arguments and modify the path of current directory

### get the current path of code
cwd = os.getcwd()
ll=sys.argv[1].split(".")
tt=str(cwd) + str(ll[1]) + "." + str(ll[2])
batchfilename=tt  #batchfile.json
if not os.path.exists(batchfilename):
    print "Input batch file is not exist"
    sys.exit()

ll=sys.argv[2].split(".")
tt=str(cwd) + str(ll[1]) + "." + str(ll[2])

streamfilename=tt  #streamfile.json
if not os.path.exists(streamfilename):
    print "Input batch file is not exist"
    sys.exit()

ll=sys.argv[3].split(".")
tt=str(cwd) + str(ll[1]) + "." + str(ll[2])

outputfilename=tt  #flagged_purchases.json

### default valuse for D and T
D=1
T=2
output_file = open(outputfilename, "w")
### Network and purchases are stored in dictionaries.
### Network dictionary: key: node id, values: negibor nodes(friends)
### Purchase dictionary: key: node id, values: list of tuples. each tuple contains date,time and amount of each purchase
Dic_Network = defaultdict(list)
Dic_purchase = defaultdict(list)

### calling make_network_purchase_dictionary with given batchfile to construct the network and purchase dictionary
Dic_Network,Dic_purchase,D,T= make_network_purchase_dictionary(batchfilename,Dic_Network,Dic_purchase,1,D,T,output_file)


### calling make_network_purchase_dictionary with given streamfile to update the network and purchase dictionary and detect the anomaly purchase
Dic_Network,Dic_purchase,D,T= make_network_purchase_dictionary(streamfilename,Dic_Network,Dic_purchase,2,int(D),int(T),output_file)
output_file.close()
