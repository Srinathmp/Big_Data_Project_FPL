#!/usr/bin/python
import findspark
findspark.init()
import time
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import  Row,SQLContext,SparkSession
from pyspark.sql.functions import col
import sys
import requests
import json
import csv


#def getMetrics(rdd):
 #   r = [json.loads(x) for x in rdd]
  #  match = r[0]     
   # events = r[1:]
   # print(match,events)
'''
teams_dict = { # key team name value team id
'Newcastle United FC':	1613,
'Huddersfield Town FC':	1673,
'Swansea City AFC':	10531,
'AFC Bournemouth':	1659,
'Brighton & Hove Albion FC':	1651,
'Burnley FC':	1646,
'Leicester City FC':	1631,
'West Ham United FC':	1633,
'Stoke City FC':	1639,
'Watford FC':	1644,
'Everton FC':	1623,
'West Bromwich Albion FC':	1627,
'Manchester City FC':	1625,
'Tottenham Hotspur FC':	1624,
'Crystal Palace FC':	1628,
'Southampton FC':	1619,
'Liverpool FC':	1612,
'Chelsea FC':	1610,
'Manchester United FC':	1611,
'Arsenal FC':	1609
}

teams_inv_dict = dict(zip(ini_dict.values(), teams_dict.keys())) # key team id value team name 
'''


conf=SparkConf()
conf.setAppName("FPL")
sc=SparkContext(master="local[4]",conf=conf)
#spark=SparkSession.builder.appName("FPL").getOrCreate()
ssc=StreamingContext(sc,5)
ssc.checkpoint("checkpoint_FPL")

dataStream=ssc.socketTextStream("localhost",6100)
csvfile=open('players.csv','r')
fieldnames=("name","birthArea","birthDate","foot","role","height","passportArea","weight","Id")
reader=csv.reader(csvfile,fieldnames)
l=[]
for row in reader:
	l.append([row[0],row[-1]])
	#print(row[0],row[-1])

l.remove(l[0])
for i in l:
	for j in range(6):
		i.append(0)
#print(l)

def tag_list(x):
	l=[]
	n=len(x["tags"])
	for i in range(n):
		l.append(x["tags"][i]['id'])
	return l
	
def anp_f(x):
	l=tag_list(x)
	if(1801 in l):
		return True
	return False
	
def akp_f(x):
	l=tag_list(x)
	if(1801 in l and 302 in l):
		return True
	return False

def np_f(x):
	l=tag_list(x)
	if(1801 in l or 1802 in l):
		return True
	return False	

def kp_f(x):
	l=tag_list(x)
	if(302 in l):
		return True
	return False

#dual 
	
def dw_f(x):
	l=tag_list(x)
	if 703 in l:
		return True
	return False
	
def dn_f(x):
	l=tag_list(x)
	if 702 in l:
		return True
	return False

def dl_f(x):
	l=tag_list(x)
	if 701 in l:
		return True
	return False
#free kick
def fk_acc(x):
	l = tag_list(x)
	if( 1801 in l):
		return True
	return False
	
def p_sc(x): # if subeventid is 35 then free kick is a penalty and if 101 in l its a scored goal
	l = tag_list(x)
	if( x["subEventId"] == 35 and 101 in l):
		return True
	return False

def sotg(x):
	l = tag_list(x)
	if(1801 in l and  101 in l):
		return True
	return False

def sotNg(x):
	l = tag_list(x)
	if(1801 in l and  101 not in l):
		return True
	return False

def ts(x):
	l = tag_list(x)
	if(1801 in l or 1802 in l):
		return True
	return False

	
#player_id=spark.read.option("header",True).csv("players.csv")
#player_ni=player_id.select(col("name"),col("Id"))
#print(player_ni)
#dataStream.pprint()
#st=eval(dataStream)
#print(type(st))


#jst = dataStream.map(lambda v:json.loads(v))

#def bifurcate_match(rdd)):
#	if('wyID' in rdd):
#		return True
#	else:
#		return False
		
#def bifurcate_event(rdd):
#	if('wyID' not in rdd):
#		return True
#	else:
#		return False
#jst.pprint()
#event_df=
mp=dataStream.map(lambda x: json.loads(x))
#fm=dataStream.flatMap(lambda x: x.split(' ')[0])
#fm.pprint()
#jst_match = dataStream.filter(lambda x: "wyID" in x)
jst_event = mp.filter(lambda y: "eventId" in y.keys())
passes=jst_event.filter(lambda p: p["eventId"]==8)
acc_normal_passes = passes.filter(anp_f)
acc_normal_passes = acc_normal_passes.map(lambda x:(x['playerId'],1))

acc_key_passes = passes.filter(akp_f)
acc_key_passes = acc_key_passes.map(lambda x:(x['playerId'],2))


numerator=acc_normal_passes.union(acc_key_passes)
numerator=numerator.reduceByKey(lambda x,y:x+y)


#akp_count=acc_key_passes.count()
#akp_count.pprint()
normal_passes=passes.filter(np_f)
normal_passes=normal_passes.map(lambda x:(x['playerId'],1))

#np_count=normal_passes.count()
key_passes=passes.filter(kp_f)
key_passes=key_passes.map(lambda x:(x['playerId'],2))

denominator=normal_passes.union(key_passes)
denominator=denominator.reduceByKey(lambda x,y:x+y)

pass_accuracy=numerator.union(denominator).reduceByKey(lambda x,y:x/y)
#pass_accuracy.pprint() 



#-------------Dual effectiveness ----------------

dual=jst_event.filter(lambda p: p["eventId"]==1)
dual_won = dual.filter(dw_f)
#dual_won.pprint(
dual_lost = dual.filter(dl_f)
dual_neutral = dual.filter(dn_f)
dual_won = dual_won.map(lambda x:(x["playerId"],1))
dual_neutral_d = dual_neutral.map(lambda x:(x["playerId"],1))
dual_neutral = dual_neutral.map(lambda x:(x["playerId"],0.5))
dual_lost = dual_lost.map(lambda x:(x["playerId"],1))
dual_numerator = dual_won.union(dual_neutral).reduceByKey(lambda x,y:x+y)
#dual_numerator.pprint()
dual_denominator = dual_won.union(dual_neutral_d)
#dual_denominator.pprint()
dual_denominator = dual_denominator.union(dual_lost).reduceByKey(lambda x,y: x+y)
#dual_denominator = dual_denominator
#dual_denominator.pprint()
dual_effectiveness=dual_numerator.union(dual_denominator).reduceByKey(lambda x,y:x/y)

#dual_effectiveness.pprint()
#--------------FreeKick Effectiveness-------------
free_kick = jst_event.filter(lambda p: p["eventId"]==3)
free_kick_accurate = free_kick.filter(fk_acc)
free_kick_accurate = free_kick_accurate.map(lambda x:(x["playerId"],1))
penalty_scored = free_kick.filter(p_sc)
penalty_scored = penalty_scored.map(lambda x:(x["playerId"],1))
free_kick_numerator = free_kick_accurate.union(penalty_scored)
free_kick_numerator = free_kick_numerator.reduceByKey(lambda x,y: x+y)

free_kick_denominator = free_kick.map(lambda x:(x["playerId"],1)).reduceByKey(lambda x,y:x+y)

freekick_effectiveness=free_kick_numerator.union(free_kick_denominator).reduceByKey(lambda x,y:x/y)
#freekick_effectiveness.pprint()




#---------------------shots on target----------------------
shots = jst_event.filter(lambda p: p["eventId"]==10)
shots_on_target_goal=shots.filter(sotg)
shots_on_target_Ngoal=shots.filter(sotNg)

shots_on_target_goal=shots_on_target_goal.map(lambda x:(x["playerId"],1))
shots_on_target_Ngoal=shots_on_target_Ngoal.map(lambda x:(x["playerId"],0.5))

shots_on_target_numarator=shots_on_target_goal.union(shots_on_target_Ngoal).reduceByKey(lambda x,y:x+y)
#total_shots=shots.map(lambda x:(x["playerId"],1)).reduceByKey(lambda x,y:x+y)
total_shots=shots.filter(ts)
total_shots=total_shots.map(lambda x:(x["playerId"],1)).reduceByKey(lambda x,y:x+y)

#temp=shots_on_target_numarator.join(total_shots)
#temp.pprint()


shots_effectiveness=shots_on_target_numarator.union(total_shots).reduceByKey(lambda x,y: x/y if y>x else y/x)
#shots_effectiveness.pprint()




#---------------------foul loss----------------------
fouls=jst_event.filter(lambda p: p["eventId"]==2)
fouls_players=fouls.map(lambda x:(x["playerId"],1)).reduceByKey(lambda x,y:x+y)



#---------------------own goals----------------------
fouls=jst_event.filter(lambda p: p["eventId"]==102)
fouls_players=fouls.map(lambda x:(x["playerId"],1)).reduceByKey(lambda x,y:x+y)


#-------------------AFTER MATCH-----------------

#-------------------Player Contribution------------

player_contribution = pass_accuracy.union(dual_effectiveness).union(freekick_effectiveness).union(shots_effectiveness).reduceByKey(lambda x,y: x+y)
player_contribution=player_contribution.reduceByKey(lambda x: x/4)
player_contribution.pprint()




ssc.start()
ssc.awaitTermination(15) #check ssc.awaitTermination() # set how may seconds we want in () 
ssc.stop()
