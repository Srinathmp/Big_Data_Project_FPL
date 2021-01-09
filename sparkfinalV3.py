#!/usr/bin/python3

import findspark
findspark.init()
import time
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import  Row,SQLContext,SparkSession
from pyspark.sql.functions import col
import pandas as pd
import sys
import requests
import json
import csv


df = pd.read_csv('chemistry.csv')
df=df.set_index("playerid")
df1= pd.read_csv('player_rating.csv')
df1=df1.set_index("playerid")






conf=SparkConf()
conf.setAppName("FPL")
sc=SparkContext(master="local[4]",conf=conf)
sqlcontext=SQLContext(sc)
ssc=StreamingContext(sc,5)
ssc.checkpoint("checkpoint_FPL")

dataStream=ssc.socketTextStream("localhost",6100)



#----sort proper---
def great(x):
	if x[1]>1:
		return True
	else:
		return False

def tag_list(x):
	l=[]
	n=len(x["tags"])
	for i in range(n):
		l.append(x["tags"][i]['id'])
	return l
	
def anp_f(x):
	l=tag_list(x)
	if(1801 in l and 302 not in l):
		return True
	else:
		return False

def akp_f(x):
	l=tag_list(x)
	if(1801 in l and 302 in l):
		return True
	else:
		return False
		
def match_file(match_object):
	mat_di={
	"Newcastle United FC":	1613,
	"Huddersfield Town FC":	1673,
	"Swansea City AFC":	10531,
	"AFC Bournemouth":	1659,
	"Brighton & Hove Albion FC":	1651,
	"Burnley FC":	1646,
	"Leicester City FC":	1631,
	"West Ham United FC":	1633,
	"Stoke City FC":	1639,
	"Watford FC":	1644,
	"Everton FC":	1623,
	"West Bromwich Albion FC":	1627,
	"Manchester City FC":	1625,
	"Tottenham Hotspur FC":	1624,
	"Crystal Palace FC":	1628,
	"Southampton FC":	1619,
	"Liverpool FC":	1612,
	"Chelsea FC":	1610,
	"Manchester United FC":	1611,
	"Arsenal FC":	1609
	}
	inv_mat = dict(zip(mat_di.values(), mat_di.keys())) 


	#m=json.loads(match_object)
	m=match_object
	match_dict={}
	match_dict['label']=m['label']
	match_dict['date']=m['dateutc'].split()[0]
	match_dict['duration']=m['duration']
	match_dict['winner']=inv_mat[m['winner']]
	match_dict['venue']=m['venue']
	match_dict['gameweek']=m['gameweek']
	
	match_dict['goals']=[]
	match_dict['own_goals']=[]
	match_dict['yellow_cards']=[]
	match_dict['red_cards']=[]
	
	#own goals
	teams=m['teamsData'].keys()
	#formation
	
	for team in teams:
		formation=m['teamsData'][team]['formation']
		bench=formation['bench']
		for player in bench:
			temp1={}
			if(player['ownGoals']>0):
				temp1['playerId']=player['playerId']
				temp1['team']=inv_mat[team]
				temp1['ownGoals']=player['ownGoals']
				match_dict['own_goals'].append(temp1)
				#temp={}
			if(player['goals']>0):
				temp2={}
				temp2['playerId']=player['playerId']
				temp2['team']=inv_mat[team]
				temp2['goals']=player['goals']
				match_dict['goals'].append(temp2)
				#temp={}
			if(player['yellowCards']>0):
				match_dict['yellow_cards'].append(player['playerId'])
			if(player['redCards']>0):
				match_dict['red_cards'].append(player['playerId'])
		
		lineup=formation['lineup']
		for player in lineup:
			temp3={}
			if(player['ownGoals']>0):
				temp3['playerId']=player['playerId']
				temp3['team']=inv_mat[team]
				temp3['ownGoals']=player['ownGoals']
				match_dict['own_goals'].append(temp3)
				#temp={}
			if(player['goals']>0):
				temp4={}
				temp4['playerId']=player['playerId']
				temp4['team']=inv_mat[team]
				temp4['goals']=player['goals']
				match_dict['goals'].append(temp4)
				#temp={}
			if(player['yellowCards']>0):
				match_dict['yellow_cards'].append(player['playerId'])
			if(player['redCards']>0):
				match_dict['red_cards'].append(player['playerId'])
			
		substitutions=formation['substitutions']
		for player in substitutions:
			temp5={}
			if(player['ownGoals']>0):
				temp5['playerId']=player['playerId']
				temp5['team']=inv_mat[team]
				temp5['ownGoals']=player['ownGoals']
				match_dict['own_goals'].append(temp5)
				#temp={}
			if(player['goals']>0):
				temp6={}
				temp6['playerId']=player['playerId']
				temp6['team']=inv_mat[team]
				temp6['goals']=player['goals']
				match_dict['goals'].append(temp6)
				#temp={}
			if(player['yellowCards']>0):
				match_dict['yellow_cards'].append(player['playerId'])
			if(player['redCards']>0):
				match_dict['red_cards'].append(player['playerId'])
				

	#match_dict['teamsData']=m['teamsData']
	match_dict=json.dumps(match_dict)
	with open('MatchData.json','a') as f:
		f.write(match_dict)
	return match_dict
	
mp=dataStream.map(lambda x: json.loads(x))


match=mp.filter(lambda y: "wyId" in y.keys()) #all match data
#matchdata=match.filter(match_file)
matchdata=match.map(lambda x: match_file(x))
#matchdata.pprint()

jst_event = mp.filter(lambda y: "eventId" in y.keys())
#jst_event.pprint()
passes=jst_event.filter(lambda p: p["eventId"]==8)
acc_normal_passes = passes.filter(anp_f)
acc_normal_passes = acc_normal_passes.map(lambda x:((x['playerId'],x['teamId']),1))

acc_key_passes = passes.filter(akp_f)
acc_key_passes = acc_key_passes.map(lambda x:((x['playerId'],x['teamId']),2))


numerator=acc_normal_passes.union(acc_key_passes)
numerator=numerator.reduceByKey(lambda x,y:x+y)

def np_f(x):
	l=tag_list(x)
	if(302 not in l):
		return True
	else:
		return False	

normal_passes=passes.filter(np_f)
normal_passes=normal_passes.map(lambda x:((x['playerId'],x['teamId']),1))
	
def less(x):
	if x[1]<=1:
		return True
	else:
		return False
	
def kp_f(x):
	l=tag_list(x)
	if(302 in l):
		return True
	else:
		return False
#np_count=normal_passes.count()
key_passes=passes.filter(kp_f)
key_passes=key_passes.map(lambda x:((x['playerId'],x['teamId']),2))

denominator=normal_passes.union(key_passes)
denominator=denominator.reduceByKey(lambda x,y:x+y)

pass_accuracy=numerator.union(denominator).reduceByKey(lambda x,y:x/y)
final1=pass_accuracy.filter(less)
final2=pass_accuracy.filter(great)
final2=final2.map(lambda x: (x[0],0))
pass_accuracy=final1.union(final2)


#pass_accuracy.pprint() 



#-------------Dual effectiveness ----------------
#dual 
	
def dw_f(x):
	l=tag_list(x)
	if 703 in l:
		return True
	else:
		return False
	
def dn_f(x):
	l=tag_list(x)
	if 702 in l:
		return True
	else:
		return False

def dl_f(x):
	l=tag_list(x)
	if 701 in l:
		return True
	else:
		return False
dual=jst_event.filter(lambda p: p["eventId"]==1)
dual_won = dual.filter(dw_f)
#dual_won.pprint()
dual_lost = dual.filter(dl_f)
dual_neutral = dual.filter(dn_f)
dual_won = dual_won.map(lambda x:((x['playerId'],x['teamId']),1))
dual_neutral_d = dual_neutral.map(lambda x:((x['playerId'],x['teamId']),1))
dual_neutral = dual_neutral.map(lambda x:((x['playerId'],x['teamId']),0.5))
dual_lost = dual_lost.map(lambda x:((x['playerId'],x['teamId']),1))
dual_numerator = dual_won.union(dual_neutral).reduceByKey(lambda x,y:x+y)
#dual_numerator.pprint()
dual_denominator = dual_won.union(dual_neutral_d)
#dual_denominator.pprint()
dual_denominator = dual_denominator.union(dual_lost).reduceByKey(lambda x,y: x+y)
#dual_denominator.pprint()
dual_effectiveness=dual_numerator.union(dual_denominator).reduceByKey(lambda x,y:x/y)
final1=dual_effectiveness.filter(less)
final2=dual_effectiveness.filter(great)
final2=final2.map(lambda x: (x[0],0))
dual_effectiveness=final1.union(final2)
#dual_effectiveness.pprint()





#--------------FreeKick Effectiveness-------------


#free kick
def fk_acc(x):
	l = tag_list(x)
	if( 1801 in l):
		return True
	else:
		return False
	
def p_sc(x): # if subeventid is 35 then free kick is a penalty and if 101 in l its a scored goal
	l = tag_list(x)
	if( x["subEventId"] == 35 and 101 in l):
		return True
	else:
		return False

def sotg(x):
	l = tag_list(x)
	if(1801 in l and  101 in l):
		return True
	else:
		return False

def sotNg(x):
	l = tag_list(x)
	if(1801 in l and  101 not in l):
		return True
	else:
		return False
	
free_kick = jst_event.filter(lambda p: p["eventId"]==3)
free_kick_accurate = free_kick.filter(fk_acc)
free_kick_accurate = free_kick_accurate.map(lambda x:((x['playerId'],x['teamId']),1))
penalty_scored = free_kick.filter(p_sc)
penalty_scored = penalty_scored.map(lambda x:((x['playerId'],x['teamId']),1))
free_kick_numerator = free_kick_accurate.union(penalty_scored)
free_kick_numerator = free_kick_numerator.reduceByKey(lambda x,y: x+y)

free_kick_denominator = free_kick.map(lambda x:((x['playerId'],x['teamId']),1)).reduceByKey(lambda x,y:x+y)

freekick_effectiveness=free_kick_numerator.union(free_kick_denominator).reduceByKey(lambda x,y:x/y)
final1=freekick_effectiveness.filter(less)
final2=freekick_effectiveness.filter(great)
final2=final2.map(lambda x: (x[0],0))
freekick_effectiveness=final1.union(final2)

#freekick_effectiveness.pprint()




#---------------------shots on target----------------------

def sotg(x):
	l = tag_list(x)
	if(1801 in l and  101 in l):
		return True
	else:
		return False

def sotNg(x):
	l = tag_list(x)
	if(1801 in l and  101 not in l):
		return True
	else:
		return False
	


def sot_f(x):
	l=tag_list(x)
	if(1801 in l):
		return True
	else:
		return False

'''
def ts(x):
	l = tag_list(x)
	if(1801 in l or 1802 in l):
		return True
	return False
'''	
shots = jst_event.filter(lambda p: p["eventId"]==10)
shots_on_target_goal=shots.filter(sotg)
shots_on_target_Ngoal=shots.filter(sotNg)

shots_on_target_goal=shots_on_target_goal.map(lambda x:((x['playerId'],x['teamId']),1))
shots_on_target_Ngoal=shots_on_target_Ngoal.map(lambda x:((x['playerId'],x['teamId']),0.5))

shots_on_target_numarator=shots_on_target_goal.union(shots_on_target_Ngoal).reduceByKey(lambda x,y:x+y)
total_shots=shots.map(lambda x:((x['playerId'],x['teamId']),1)).reduceByKey(lambda x,y:x+y)

	
shots_effectiveness=shots_on_target_numarator.union(total_shots).reduceByKey(lambda x,y: x/y)
final1=shots_effectiveness.filter(less)
final2=shots_effectiveness.filter(great)
final2=final2.map(lambda x: (x[0],0))
shots_effectiveness=final1.union(final2)
#shots_effectiveness.pprint()

def new(nv,old):
	return sum(nv)+(old or 0)

def g_f(x):
	l=tag_list(x)
	if(101 in l):
		return True
	else:
		return False

goals=shots.filter(g_f)
goals=goals.map(lambda x:((x['playerId'],x['teamId']),1)).reduceByKey(lambda x,y:x+y)
goals=goals.updateStateByKey(new)
sot=shots.filter(sot_f)
sot=sot.map(lambda x:((x['playerId'],x['teamId']),1)).reduceByKey(lambda x,y:x+y)
sot=sot.updateStateByKey(new)
#sot.pprint()


#---------------------foul loss----------------------
fouls=jst_event.filter(lambda p: p["eventId"]==2)
fouls_players=fouls.map(lambda x:((x['playerId'],x['teamId']),1)).reduceByKey(lambda x,y:x+y)
fouls_players=fouls_players.updateStateByKey(new)
#fouls_players.pprint()

#---------------------own goals----------------------

def og_f(x):
	l=tag_list(x)
	if(102 in l):
		return True
	return False

own_goals=jst_event.filter(og_f)
#own_goals.pprint()
own_goals_players=own_goals.map(lambda x:((x['playerId'],x['teamId']),1)).reduceByKey(lambda x,y:x+y)
own_goals_players=own_goals_players.updateStateByKey(new)
#own_goals_players.pprint()

#-------------------AFTER MATCH-----------------

#-------------------Player Contribution------------

player_contribution = pass_accuracy.union(dual_effectiveness).union(freekick_effectiveness).union(shots_effectiveness).reduceByKey(lambda x,y: x+y)
player_contribution=player_contribution.map(lambda x: (x[0],x[1]/4))


match = mp.filter(lambda y: "wyId" in y.keys())
date = match.map(lambda x: x['dateutc'].split(' ')[0])
players = match.map(lambda x:list(x['teamsData'].values()))
players = players.map(lambda x:list(((x[0]['formation'],x[0]['teamId']),(x[1]['formation'],x[1]['teamId']))))
substitutions = players.map(lambda x: list(((x[0][0]['substitutions'],x[0][1]),(x[1][0]['substitutions'],x[1][1]))))
#substitutions.pprint()

sub_playerin=substitutions.map(lambda x: list((list((((x[0][0][j]['playerIn'],x[0][1]),(90-x[0][0][j]['minute'])/90)for j in range(len(x[0][0])))),list((((x[1][0][j]['playerIn'],x[1][1]),(90-x[1][0][j]['minute'])/90)for j in range(len(x[1][0])))))))
sub_playerin=sub_playerin.map(lambda x: (x[0]+x[1]))
sub_playerin=sub_playerin.flatMap(lambda x: x)
#sub_playerin.pprint()

def equal2(x):
	if x[1]==2:
		return True
	return False

sub_playerout=substitutions.map(lambda x: list((list((((x[0][0][j]['playerOut'],x[0][1]),(90-x[0][0][j]['minute'])/90)for j in range(len(x[0][0])))),list((((x[1][0][j]['playerOut'],x[1][1]),(90-x[1][0][j]['minute'])/90)for j in range(len(x[1][0])))))))
sub_playerout=sub_playerout.map(lambda x: (x[0]+x[1]))
sub_playerout=sub_playerout.flatMap(lambda x: x)

sub_player_with_normal=sub_playerout.union(sub_playerin)

temp=sub_player_with_normal.map(lambda x: (x[0],1))
temp1=temp.union(player_contribution).reduceByKey(lambda x,y:x+y)
non_sub_normal=temp1.filter(less)
non_sub_normal=non_sub_normal.map(lambda x: (x[0],x[1]*1.05))
player_contribution_normal=sub_player_with_normal.union(non_sub_normal)

bench = players.map(lambda x: list(((x[0][0]['bench'],x[0][1]),(x[1][0]['bench'],x[1][1]))))
bench_players=bench.map(lambda x: list((list(((x[0][0][j]['playerId'],x[0][1])for j in range(len(x[0][0])))),list(((x[1][0][j]['playerId'],x[1][1])for j in range(len(x[1][0])))))))
bench_players = bench_players.map(lambda x: (x[0]+x[1]))
bench_players = bench_players.flatMap(lambda x: x)

bench_players_normal=bench_players.map(lambda x: (x,2))

temp=player_contribution_normal.union(bench_players_normal).reduceByKey(lambda x,y:x+y)
temp1=temp.filter(equal2)
bench_only=temp1.map(lambda x:(x[0],0))


player_contribution_normal=player_contribution_normal.union(bench_only)
all_players=player_contribution.map(lambda x:(x[0],0))

#player_contribution_normal.pprint()

#---------------------------------Player performance--------------------------

fouls_players1=fouls_players.map(lambda x: (x[0],x[1]*0.005))
own_goals_players1=own_goals_players.map(lambda x: (x[0],x[1]*0.05))
reduce=own_goals_players1.union(fouls_players1).reduceByKey(lambda x,y: x+y)
#reduce.pprint()

temp=reduce.map(lambda x: (x[0],2))
temp=temp.union(player_contribution_normal).reduceByKey(lambda x,y:x+y)
temp1=temp.filter(lambda x: x[1]>=2)
temp1=temp1.map(lambda x:(x[0],x[1]-2))

red_final=temp1.union(reduce).reduceByKey(lambda x,y:x*y)
player_contribution_reduce=player_contribution_normal.union(red_final).reduceByKey(lambda x,y:x-y)
final1=player_contribution_reduce.filter(lambda x: x[1]>=0)
final2=player_contribution_reduce.filter(lambda x: x[1]<0)
final2=final2.map(lambda x: (x[0],-x[1]))
player_contribution_reduce=final1.union(final2)

#player_contribution_reduce.pprint()

#------------------------------Player rating -------------------------------


def player_rating(new_rating,old_rating):
	return (sum(new_rating)+(old_rating or 0.5))/2
	
def oldandnew(new,old):
	return (new,old or 0.5)
	
player_r = player_contribution_reduce.updateStateByKey(player_rating)
#player_r.pprint()

player_r_less5=player_r.map(lambda x: (x[0],1))
player_r_less5=player_r_less5.updateStateByKey(new)
player_r_less5=player_r_less5.filter(lambda x: x[1]<5) # for players who played less than 5 matches
player_r_less5=player_r_less5.map(lambda x: (x[0],0))
player_r1=player_r_less5.leftOuterJoin(player_r).map(lambda x: (x[0],x[1][1]))


player_r_less5.pprint()

#------------------------------player chemistry-----------------------------
tempo=player_r.map(lambda x: (1,x))
chem=tempo.join(tempo)
chem=chem.map(lambda x: x[1])
same_team=chem.filter(lambda x: x[0][0][1]==x[1][0][1])
opp_team=chem.filter(lambda x: x[0][0][1]!=x[1][0][1])


def same(x):
	global df1
	pid_1=x[0][0][0]
	pid_2=x[1][0][0]
	rating1=x[0][1]
	rating2=x[1][1]
	if(pid_1==0 or pid_2==0):
		return ((pid_1,pid_2),0)
	oldr1=df1.loc[pid_1,'player_rating']
	oldr2=df1.loc[pid_2,'player_rating']
	if(((oldr1 > rating1) and (oldr2 > rating2)) or ((oldr1 < rating1) and (oldr2 < rating2))): # 1 dec 2 dec 
		new_chemistry = abs(abs(oldr1-rating1)-abs(oldr2-rating2))/2
		df.loc[pid_1,str(pid_2)]+=new_chemistry #-----------------------------------
		return ((pid_1,pid_2),new_chemistry)
	else:
		new_chemistry = abs(abs(oldr1-rating1)-abs(oldr2-rating2))/2
		df.loc[pid_1,str(pid_2)]-=new_chemistry
		return ((pid_1,pid_2),-new_chemistry)
		
def opp(x):
	global df1
	pid_1=x[0][0][0]
	pid_2=x[1][0][0]
	rating1=x[0][1]
	rating2=x[1][1]
	if(pid_1==0 or pid_2==0):
		return ((pid_1,pid_2),0)
	oldr1=df1.loc[pid_1,'player_rating']
	oldr2=df1.loc[pid_2,'player_rating']
	
	if(((oldr1 > rating1) and (oldr2 > rating2)) or ((oldr1 < rating1) and (oldr2 < rating2))): # 1 dec 2 dec 
		new_chemistry = abs(abs(oldr1-rating1)-abs(oldr2-rating2))/2
		df.loc[pid_1,str(pid_2)]-=new_chemistry
		return ((pid_1,pid_2),-new_chemistry)
	else:
		new_chemistry = abs(abs(oldr1-rating1)-abs(oldr2-rating2))/2
		df.loc[pid_1,str(pid_2)]+=new_chemistry
		return ((pid_1,pid_2),new_chemistry)

#same_team.pprint()
#def chem(new,old):
#	return sum(new)+(old or 0.5)

chemistry_same=same_team.map(same)
#chemistry_same=chemistry_same.updateStateByKey(chem)
chemistry_opp=opp_team.map(opp)
#chemistry_opp=chemistry_opp.updateStateByKey(chem)
#chemistry_same.pprint() 



pass_accuracy=pass_accuracy.union(all_players).reduceByKey(lambda x,y: x+y)
goals=goals.union(all_players).reduceByKey(lambda x,y: x+y)
fouls_players=fouls_players.union(all_players).reduceByKey(lambda x,y: x+y)
own_goals_players=own_goals_players.union(all_players).reduceByKey(lambda x,y: x+y)
sot=sot.union(all_players).reduceByKey(lambda x,y: x+y)

player_profile=pass_accuracy.join(fouls_players)
player_profile=player_profile.join(goals)
player_profile=player_profile.join(own_goals_players)
player_profile=player_profile.join(sot)
player_profile=player_profile.map(lambda x: (x[0],(x[1][0][0][0][0],x[1][0][0][0][1],x[1][0][0][1],x[1][0][1],x[1][1])))
#player_p=player_profile.map(lambda x: (x,1))
#player_profile.saveAsTextFiles("player_profile.txt")


def prcsv(rec):
	global df1
	if rec[0][0]==0:
		return False
	df1.loc[rec[0][0],"player_rating"]=rec[1]
	df1.to_csv("player_rating.csv")
	return True


player_r = player_r.filter(prcsv)
#player_r.saveAsTextFiles("player_rating.txt")
#chemistry_same.saveAsTextFiles("chemistry_same.txt")
#chemistry_opp.saveAsTextFiles("chemistry_opp.txt")

'''def tpprint_chemistry_same(val,num=50):
    def takeAndPrint(time, rdd):
        taken = rdd.take(num + 1)
        for record in taken[:num]:
            with open("chemistry_same_merge.txt", "a") as myfile:
            	myfile.write(str(record)+';')

    val.foreachRDD(takeAndPrint)
    
def tpprint_chemistry_opp(val,num=50):
    def takeAndPrint(time, rdd):
        taken = rdd.take(num + 1)
        for record in taken[:num]:
            with open("chemistry_opp_merge.txt", "a") as myfile:
                myfile.write(str(record)+';')

    val.foreachRDD(takeAndPrint)'''
    
def tpprint_player_profile(val,num=50):
    def takeAndPrint(time, rdd):
        taken = rdd.take(num + 1)
        for record in taken[:num]:
            with open("player_profile_merge.csv", "a") as myfile:
            	writer=csv.writer(myfile)
            	writer.writerow(list(record[1]))

    val.foreachRDD(takeAndPrint)

def tpprint_rating(val,num=50):
    def takeAndPrint(time, rdd):
        taken = rdd.take(num + 1)
        for record in taken[:num]:
            with open("players_rating.csv", "a") as myfile:
            	writer=csv.writer(myfile)
            	writer.writerow(list(record))

    val.foreachRDD(takeAndPrint)

def tpprint_chem(val,num=50):
    def takeAndPrint(time, rdd):
        taken = rdd.take(num + 1)
        for record in taken[:num]:
            with open("my_chem.csv", "a") as myfile:
            	writer=csv.writer(myfile)
            	writer.writerow(list(record))

    val.foreachRDD(takeAndPrint)

player_r=player_r.reduceByKey(lambda x,y: (x+y)/2)
player_rr=player_r.map(lambda x: (x[0][0],x[1]))
player_rr=player_rr.map(lambda x: (1,x))
date =date.map(lambda x: (1,x))
player_rr=player_rr.join(date)
player_rr=player_rr.map(lambda x: (x[1][0][0],x[1][0][1],x[1][1]))
#player_rr.pprint()
tpprint_rating(player_rr)
#tpprint_chemistry_same(chemistry_same)
#tpprint_chemistry_opp(chemistry_opp)
player_profile1=player_profile.map(lambda x: (x[0][0],(x[0][0],x[1][0],x[1][1],x[1][2],x[1][3],x[1][4])))
tpprint_player_profile(player_profile1)


all_chemistry = chemistry_same.union(chemistry_opp)
all_chemistry = all_chemistry.map(lambda x:(x[0][0],x[0][1],x[1])) 
tpprint_chem(all_chemistry)
#all_chemistry.pprint()
ssc.start()
ssc.awaitTermination(250) #check ssc.awaitTermination() # set how many seconds we want in () 
ssc.stop()





