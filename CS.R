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
head(uniq_summons_2015) # 10951256

uniq_summons_2016 <- SparkR::sql("SELECT count(DISTINCT `Summons Number`) FROM Parking_2016_view")
head(uniq_summons_2016) # 10626899

uniq_summons_2017 <- SparkR::sql("SELECT count(DISTINCT `Summons Number`) FROM Parking_2017_view")
head(uniq_summons_2017) # 10803028

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
                           # (including unknown - 99 and states from other countries such as British Columbia (BC))


#3.	Some parking tickets don’t have addresses on them, which is cause for concern. Find out how many such tickets there are.


