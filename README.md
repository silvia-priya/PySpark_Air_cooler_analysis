# PySpark_Air_cooler_analysis
This exercise performs a set of analysis on the data using pyspark

File Semantics:
---------------

Buiding.csv

BuildingID -- Unique ID for every building
BuildingMgr -- Manager of the building
BuildingAge -- Age of the building
HVACproduct -- Product identifier
Country -- Country where the product is installed.
HVAC.csv

Date -- Date at which the temperature is recorded 
Time -- Time at which the temperature is recroded 
TargetTemp -- Final temperature recorded after applying the cooling agent 
ActualTemp -- Actual temperatur set 
System -- System id 
SystemAge -- Age of the system 
BuildingID -- Unique ID for every building

The building ID is the unique column and the above two files needs to be joined using the common field BuildingID

Use cases:-
-----------

1.Find out the top 2 countries topped where the delta temperature(difference between actual and target temperature) is >5 
2.Find out the top 2 countries topped where the delta temperature(difference between actual and target temperature) is <5
