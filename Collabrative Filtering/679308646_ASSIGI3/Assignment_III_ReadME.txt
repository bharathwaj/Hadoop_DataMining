*************************************************************************
ASSIGNMENT III item bases recomender systems
*************************************************************************
File List:
CollabFiltDriver
CollabFiltMapper1
CollabFiltMapper2
CollabFiltReducer1
CollabFiltReducer2
ValueComparator
WriteFile



Package:
edu.uic.ids561.hadoop.collfilter.driver


Input:
u2.data in the folder
/user/hduser


Final Output:
Two out put folders are created with two success files 
one contains the intermediate input for second job and the secoud foder consists of 
cosinie similarity for the films
The 100 recomendation is present in the file output.txt

Optional:The number can be varied by giving appropriate number in Driver 


Hadoop:
 hadoop jar ColabFilter.jar edu.uic.ids561.hadoop.collfilter.driver.CollabFiltDriver

