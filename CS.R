Sys.setenv(SPARK_HOME = "/usr/local/spark")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "yarn")
Parking_2015 <- SparkR::read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2015.csv", header=T, "csv")
Parking_2016 <- SparkR::read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2016.csv", header=T, "csv")
Parking_2017 <- SparkR::read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", header=T, "csv")
# As a next step, lets validate the number of records in each of the 3 files.
count(Parking_2015) #11809233
count(Parking_2016) #10626899
count(Parking_2017) #10803028
# Next lets check the number of columns - 
ncol(Parking_2015) # 51 columns
ncol(Parking_2016) # 51 columns
ncol(Parking_2017) # 43 columns - this dataset is different than the other 2. The last 8 columns - present in 2015 and 2016
                   # is not present in this file.
#Create views
createOrReplaceTempView(Parking_2015, "Parking_2015_view")
createOrReplaceTempView(Parking_2016, "Parking_2016_view")
createOrReplaceTempView(Parking_2017, "Parking_2017_view")

# Since the analysis is to be done for the 3 years, we chose the fiscal year as per the files for the analysis (as 
# mentioned in the guidelines) and ran the scripts/queries for each of the files separately in most cases.

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
violation_by_Precinct_2015 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2015,`Violation Precinct`
	                                  FROM Parking_2015_view 
	                                GROUP BY `Violation Precinct`
	                                ORDER BY COUNT(1) desc")
head(violation_by_Precinct_2015)
#   Total_Violations_2015 Violation Precinct                                      
# 1               1799170                  0 - most of the violations.
# 2                598351                 19
# 3                427510                 18
# 4                409064                 14
# 5                329009                  1
# 6                320963                114

# Similarly for 2016 and 2017

violation_by_Precinct_2016 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2016,`Violation Precinct`
	                                  FROM Parking_2016_view 
	                                GROUP BY `Violation Precinct`
	                                ORDER BY COUNT(1) desc")
head(violation_by_Precinct_2016)

#   Total_Violations_2016 Violation Precinct                                      
# 1               1868655                  0 - most of the violations.
# 2                554465                 19
# 3                331704                 18
# 4                324467                 14
# 5                303850                  1
# 6                291336                114

violation_by_Precinct_2017 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2017,`Violation Precinct`
	                                  FROM Parking_2017_view 
	                                GROUP BY `Violation Precinct`
	                                ORDER BY COUNT(1) desc")
head(violation_by_Precinct_2017)
#   Total_Violations_2017 Violation Precinct                                      
# 1               2072400                  0 - most of the violations.
# 2                535671                 19
# 3                352450                 14
# 4                331810                  1
# 5                306920                 18
# 6                296514                114

#Similar analysis can be done for issuing Precinct -
violation_by_Issuer_Precinct_2015 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2015,`Issuer Precinct`
	                                  FROM Parking_2015_view 
	                                GROUP BY `Issuer Precinct`
	                                ORDER BY COUNT(1) desc")
head(violation_by_Issuer_Precinct_2015)
#   Total_Violations_2015 Issuer Precinct                                         
# 1               2037745               0
# 2                579998              19
# 3                417329              18
# 4                392922              14
# 5                318778               1
# 6                314437             114


violation_by_Issuer_Precinct_2016 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2016,`Issuer Precinct`
	                                  FROM Parking_2016_view 
	                                GROUP BY `Issuer Precinct`
	                                ORDER BY COUNT(1) desc")
head(violation_by_Issuer_Precinct_2016)
#   Total_Violations_2016 Issuer Precinct                                         
# 1               2140274               0
# 2                540569              19
# 3                323132              18
# 4                315311              14
# 5                295013               1
# 6                286924             114

violation_by_Issuer_Precinct_2017 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2017,`Issuer Precinct`
	                                  FROM Parking_2017_view 
	                                GROUP BY `Issuer Precinct`
	                                ORDER BY COUNT(1) desc")
head(violation_by_Issuer_Precinct_2017)
#   Total_Violations_2017 Issuer Precinct                                         
# 1               2388479               0
# 2                521513              19
# 3                344977              14
# 4                321170               1
# 5                296553              18
# 6                289950             114

# 4.	Find the violation code frequency across 3 precincts which have issued the most number of tickets - 
# do these precinct zones have an exceptionally high frequency of certain violation codes? 
# Are these codes common across precincts?

# For year 2015 and 2016 - Issuer Precincts (top 3) are same - 0, 19 and 18. For 2017, it is 0,19 and 14. 
# We can see that 0 and 19 are common across the 3 years.
# Now lets see what are the top violation codes in each of these top Precincts (that have issued highest tickets)

violation_by_code_Issuer_Precinct_2015 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2015,`Violation Code`,
	                                                   `Issuer Precinct`
	                                  FROM Parking_2015_view 
	                                 WHERE `Issuer Precinct` in ('0','19','18') 
	                                GROUP BY `Violation Code`,`Issuer Precinct`
	                                ORDER BY COUNT(1) desc,`Issuer Precinct`")
# Lets check the top 10 violoation codes for year 2015 for the top issuing precincts - 0,19 and 18
head(violation_by_code_Issuer_Precinct_2015,10)
#    Total_Violations_2015 Violation Code Issuer Precinct                         
# 1                 839197             36               0
# 2                 719745              7               0
# 3                 224516              5               0
# 4                 211975             21               0
# 5                 129079             14              18
# 6                  97154             38              19
# 7                  85007             37              19
# 8                  64133             14              19
# 9                  60618             69              18
# 10                 60215             21              19

# Similarly for 2016 and 2017 
violation_by_code_Issuer_Precinct_2016 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2016,`Violation Code`,
	                                                   `Issuer Precinct`
	                                  FROM Parking_2016_view 
	                                 WHERE `Issuer Precinct` in ('0','19','18') 
	                                GROUP BY `Violation Code`,`Issuer Precinct`
	                                ORDER BY COUNT(1) desc,`Issuer Precinct`")
# Lets check the top 10 violoation codes for year 2016 for the top issuing precincts - 0,19 and 18
head(violation_by_code_Issuer_Precinct_2016,10)
#    Total_Violations_2016 Violation Code Issuer Precinct                         
# 1                1253511             36               0
# 2                 492469              7               0
# 3                 237174             21               0
# 4                 112376              5               0
# 5                  99857             14              18
# 6                  77183             38              19
# 7                  75641             37              19
# 8                  73016             46              19
# 9                  61742             14              19
# 10                 58719             21              19
# For 2017, it is 0,19 and 14. 
violation_by_code_Issuer_Precinct_2017 <- SparkR::sql("SELECT COUNT(1) as Total_Violations_2017,`Violation Code`,
	                                                   `Issuer Precinct`
	                                  FROM Parking_2017_view 
	                                 WHERE `Issuer Precinct` in ('0','19','14') 
	                                GROUP BY `Violation Code`,`Issuer Precinct`
	                                ORDER BY COUNT(1) desc,`Issuer Precinct`")
# Lets check the top 10 violoation codes for year 2017 for the top issuing precincts - 0,19 and 14
head(violation_by_code_Issuer_Precinct_2017,10)
#    Total_Violations_2017 Violation Code Issuer Precinct                         
# 1                1400614             36               0
# 2                 516389              7               0
# 3                 268591             21               0
# 4                 145642              5               0
# 5                  86390             46              19
# 6                  73837             14              14
# 7                  72437             37              19
# 8                  72344             38              19
# 9                  58026             69              14
# 10                 57563             14              19

#############
# OBSERVATION - We can see that Violation codes 36(highest) and 7 are the most common across the 3 years.
#############

####################################
#For further Analysis, we are going to merge the 3 files, since all requirements can be achieved by merged data.
# No need to repeat the queries for each year as we are concentrating on the whole data in the below scripts.
####################################
Parking_2015 <- Parking_2015[,1:43] # Only picking first 43 columns - since 2017 file doesnt have the last 8 (last 8 columns seem insignficant)
Parking_2016 <- Parking_2016[,1:43]
Parking_All <- union(Parking_2015,Parking_2016)
Parking_All <- union(Parking_All, Parking_2017)
count(Parking_All) # Validate to see count matches the sum of the 3 files.It is [1] 33239160
# 11809233 + 10626899 + 10803028 = 33239160, matches the sum of the 3 files.

# Now, lets remove duplicates on Summons Number - there are duplicates across the 3 files as well.
Parking_All <- dropDuplicates(Parking_All, 'Summons Number')
count(Parking_All) # 32156308 records.

# Going forward, all the remaining analysis will be done on the merged file since we need to perform analaysis for times of day / seasons etc..

# 5.	You’d want to find out the properties of parking violations across different times of the day:
# o	The Violation Time field is specified in a strange format. 
#   Find a way to make this into a time attribute that you can use to divide into groups.
# o	Find a way to deal with missing values, if any.
# o	Divide 24 hours into 6 equal discrete bins of time. 
#   The intervals you choose are at your discretion. For each of these groups, 
#   find the 3 most commonly occurring violations
# o	Now, try another direction. For the 3 most commonly occurring violation codes, 
#   find the most common times of day (in terms of the bins from the previous part)

# All the above can be achieved by adding a new column which can take 7 values - >0000 to <=0400 - 12to4_AM, 
# k4000 to <=0800 - 3to6AM and so on till >2000 to <=0000 - 8to12AM

createOrReplaceTempView(Parking_All,"Parking_All_view")
mising_Violation_time_Parking_All <- SparkR::sql("SELECT count(`Summons Number`) 
	                                                FROM Parking_All_view
	                                               WHERE `Violation Time` is NULL")
head(mising_Violation_time_Parking_All) # 3084 records have null timestamp - this is very less compared to the dataset size.
# Lets group them into a different (Seventh) bucket.


