# shortest_path
a Map Reduce program to find the shortest path among web pages

The shortest_path.jar file can be used in the following way:(run it on hadoop cluster)
hadoop jar shortest_path.jar shortest_path.shortest_path input_path output_path intermediate_path source_page destination_page
  
-parameters
    input_path is the folder containing input file(s)
    output_path is the folder containing final output file(s)
    intermediate_path is the folder containing all intermediate files generated during the run
    source_page is the source webpage id 
    destination_page is the destination webpage id 
-input files format
    page_id_1: page1_neighbor1, page1_neighbor2,.....
    page_id_2: page2_neighbor1, page2_neighbor2,.....
    .......
    
-output files format
    page_id, source_destination_path, neighbors
