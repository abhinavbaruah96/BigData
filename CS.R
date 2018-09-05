Sys.setenv(SPARK_HOME = "/usr/local/spark")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "yarn")
Parking_2015 <- SparkR::read.df("/common_folder/nyc_parking/Parking_Violation_Issued_-_Fiscal_Year_2015.csv", header=T, "csv")
Parking_2016 <- SparkR::read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2016.csv", header=T, "csv")
Parking_2017 <- SparkR::read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", header=T, "csv")
# As a next step, lets validate the number of records in each of the 3 files.
count(Parking_2015) #11809233
count(Parking_2016) #10626899
count(Parking_2017) #10803028
# Examine the data
###################
#1.	Find total number of tickets for each year.
# Unique occurences of Summons number
uniq_summons_2015 <- SparkR::sql("SELECT count(DISTINCT `Summons Number`) FROM Parking_2015_view")
head(uniq_summons_2015) # 10951256  -- different than the count (*), which points that there are duplicates on Summons Number

uniq_summons_2016 <- SparkR::sql("SELECT count(DISTINCT `Summons Number`) FROM Parking_2016_view")
head(uniq_summons_2016) # 10626899

uniq_summons_2017 <- SparkR::sql("SELECT count(DISTINCT `Summons Number`) FROM Parking_2017_view")
head(uniq_summons_2017) # 10803028

# It seems that for 2015 file, there are some records which are duplicates on Summons number. 
# Files for 2016 and 2017 have no duplicates on Summons number

# Lets remove the duplicates in 2015 file and store it back in our Parking_2015 dataframe - 

Parking_2015 <- dropDuplicates(Parking_2015, 'Summons Number')


#2.	Find out how many unique states the cars which got parking tickets came from.

uniq_states_2015 <- SparkR::sql("SELECT DISTINCT `Registration State` FROM Parking_2015_view order by `Registration State`")
head(uniq_states_2015,100) # We see states such as BC (British Columbia - Canadian province) and 99 - which seems like "Not Known"

# including the observations such as BC and 99. The total number of states are : - 
uniq_states_count_2015 <- SparkR::sql("SELECT count(DISTINCT `Registration State`) FROM Parking_2015_view")
head(uniq_states_count_2015) # 69

# Similarly for 2016 and 2017 :-
uniq_states_count_2016 <- SparkR::sql("SELECT count(DISTINCT `Registration State`) FROM Parking_2016_view")
head(uniq_states_count_2016) # 68

uniq_states_count_2017 <- SparkR::sql("SELECT count(DISTINCT `Registration State`) FROM Parking_2017_view")
head(uniq_states_count_2017) # 67

# We can combine the 3 files using SQL union and see the unique list among all 3 -
uniq_states_all3 <- SparkR::sql("SELECT DISTINCT `Registration State` FROM Parking_2015_view union 
	                                   SELECT DISTINCT `Registration State` FROM Parking_2016_view union 
	                                   SELECT DISTINCT `Registration State` FROM Parking_2017_view")

head(uniq_states_all3,100) # We see the same list as present in 2015 file. 69 states in total 
                           # (including unknown("99") and states from other countries such as Canada - British Columbia (BC))


#3.	Some parking tickets don’t have addresses on them, which is cause for concern. Find out how many such tickets there are.
# This should ideally mean the address of the location where the violation occured, to cite as evidence.
# Here, as a team we have assumed that Street Name is a bare necessity and we are checking if that is present or not.
# As the major requirement of this case study is also to compare across the 3 years, lets run the count queries in each of 
# the 3 files seperately.

# In 2015 file - 

blank_street_address_2015 <- SparkR::sql("SELECT *
	                                              FROM Parking_2015_view 
	                                             WHERE `Street Name` is NULL 
	                                                OR `Street Name`= '<NA>' ")
head(blank_street_address_2015) # We see some sample data where street name is not present.
#Lets count the number of such records.
count(blank_street_address_2015) #6055 Records.

# In 2016 file - 
blank_street_address_2016 <- SparkR::sql("SELECT *
	                                              FROM Parking_2016_view 
	                                             WHERE `Street Name` is NULL 
	                                                OR `Street Name`= '<NA>' ")
count(blank_street_address_2016) # 8274 Records.

# In 2017 file - 
blank_street_address_2017 <- SparkR::sql("SELECT *
	                                              FROM Parking_2017_view 
	                                             WHERE `Street Name` is NULL 
	                                                OR `Street Name`= '<NA>' ")
count(blank_street_address_2017) # 4009 Records.

# Hence, we can see that 6055 records in 2015 file, 8274 in 2016 file and 4009 records in 2017 file dont have Stree Name 
# and thus as per our initial assumption are parking tickets that dont have an address on them.

#Aggregation tasks
##################

# The primary requirement is to do the analysis for all the 3 years and then compare the metrics across.
# Hence, as a group we have decided that most of the data analysis will be done individually on each of the 3 files.
#
# 1.	How often does each violation code occur? (frequency of violation codes - find the top 5)
violation_frequency_2015 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2015,`Violation Code`
	                                  FROM Parking_2015_view 
	                                GROUP BY `Violation Code`
	                                ORDER BY COUNT(1) desc")
head(violation_frequency_2015)
#########################
#   Total_Violations_2015 Violation Code                                          
# 1               1630912             21
# 2               1418627             38
# 3                988469             14
# 4                839197             36
# 5                795918             37
# 6                719753              7
#########################

# Similarly for 2016 and 2017 as well -

violation_frequency_2016 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2016,`Violation Code`
	                                  FROM Parking_2016_view 
	                                GROUP BY `Violation Code`
	                                ORDER BY COUNT(1) desc")
head(violation_frequency_2016)

#########################
#   Total_Violations_2016 Violation Code                                          
# 1               1531587             21
# 2               1253512             36
# 3               1143696             38
# 4                875614             14
# 5                686610             37
# 6                611013             20
#########################

violation_frequency_2017 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2017,`Violation Code`
	                                  FROM Parking_2017_view 
	                                GROUP BY `Violation Code`
	                                ORDER BY COUNT(1) desc")
head(violation_frequency_2017)
#########################
#   Total_Violations_2017 Violation Code                                          
# 1               1528588             21
# 2               1400614             36
# 3               1062304             38
# 4                893498             14
# 5                618593             20
# 6                600012             46
#########################

####
# 2.	How often does each vehicle body type get a parking ticket? How about the vehicle make? (find the top 5 for both)
####

violation_by_type_make_2015 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2015,`Vehicle Body Type`,`Vehicle Make`
	                                  FROM Parking_2015_view 
	                                GROUP BY `Vehicle Body Type`,`Vehicle Make`
	                                ORDER BY COUNT(1) desc")
head(violation_by_type_make_2015)
#   Total_Violations_2015 Vehicle Body Type Vehicle Make                          
# 1                654105               VAN         FORD
# 2                557817              4DSD        TOYOT
# 3                461886              4DSD        NISSA
# 4                459878              4DSD        HONDA
# 5                443357              SUBN        HONDA
# 6                438078              SUBN        TOYOT
violation_by_type_make_2016 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2016,`Vehicle Body Type`,`Vehicle Make`
	                                  FROM Parking_2016_view 
	                                GROUP BY `Vehicle Body Type`,`Vehicle Make`
	                                ORDER BY COUNT(1) desc")
head(violation_by_type_make_2016)
#   Total_Violations_2016 Vehicle Body Type Vehicle Make                          
# 1                584599               VAN         FORD
# 2                520598              4DSD        TOYOT
# 3                429120              SUBN        TOYOT
# 4                421460              SUBN        HONDA
# 5                420642              4DSD        HONDA
# 6                412884              4DSD        NISSA

violation_by_type_make_2017 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2016,`Vehicle Body Type`,`Vehicle Make`
	                                  FROM Parking_2017_view 
	                                GROUP BY `Vehicle Body Type`,`Vehicle Make`
	                                ORDER BY COUNT(1) desc")
head(violation_by_type_make_2017)
#   Total_Violations_2016 Vehicle Body Type Vehicle Make                          
# 1                543683              4DSD        TOYOT
# 2                535545               VAN         FORD
# 3                460332              SUBN        TOYOT
# 4                453586              4DSD        HONDA
# 5                448744              SUBN        HONDA
# 6                443074              4DSD        NISSA

# 3.	A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequencies of:
#     1.	Violating Precincts (this is the precinct of the zone where the violation occurred). Using this, can you make any insights for parking violations in any specific areas of the city? 
#     2.	Issuing Precincts (this is the precinct that issued the ticket)

