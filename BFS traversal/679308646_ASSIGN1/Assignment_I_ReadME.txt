*************************************************************************
ASSIGNMENT I Breadth First Search using Hadoop 
*************************************************************************
File List:
BFSTreeJob.java
BFSTreeMapper.java
BFSTreePartitioner.java
BFSTreeReducer.java
Counter.java
GraphStats.java
Node.java


Package:
edu.uic.ids561.hadoop


Input:
Input.txt


Final Output:
Miltiple Folders containing text files for the each traversal of the node.
The final output files cantains all the nodes colored to black 

For executing the above input file we got 11 iterations 

Hadoop:
bin/hadoop jar TREE_DFS	ASSII.jar edu.uic.ids561.hadoop.BFSTreeJob  input-dir output-dir

