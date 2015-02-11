package shortest_path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * version 1.0 
 * read nodes from text file, then output the first iteration
 * incomplete version
 */
public class shortest_path {

	static final String INF = "INF";
	static final String NA = "N/A";
	
	static final String INPUT_FILE = "sample_test.txt";
	static final String TEMP_FILE = "temp_conv.txt";
	
	
	public static class Map extends Mapper<Text, Text, Text, Text>{
		
		protected void map(Text key, Text value, Context context) 
				throws IOException ,InterruptedException {
			String[] pages;
			String[] neighbors;
			String page, dist, path, updatePath;
			
			int distance;
			//Split value to distance, path and neighbors
			pages = value.toString().split(",");
			neighbors = pages[2].split(" ");
			context.write(key, value);
			dist=pages[0];
			path=pages[1];
			if(!dist.equals(INF)){
				//starting at 1, because first parsed params is null
				for(int i=1; i<neighbors.length; i++){
					//output all the neighbors, and distance + 1
					distance = Integer.parseInt(dist)+1;
					updatePath = ":"+key+path;
					page = String.valueOf(distance)+","+updatePath+","+NA;
					context.write(new Text(neighbors[i]), new Text(page));
				}
			}			
			
		};
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		
		protected void reduce(Text key, Iterable<Text> pages, Context context) 
				throws IOException ,InterruptedException {
			String[] params;
			String dist;
			String path = ":";
			String neighbor = NA;
			String record;
			int min = Integer.MAX_VALUE; 
			int tempDis;
			System.out.println("Key = " + key.toString());
			for(Text page: pages){
				params = page.toString().split(",");
				System.out.println(page.toString());
				if(!params[2].equals(NA)){
					neighbor = params[2];
					System.out.println("copy "+params[2]+ "to neighbor = "+neighbor);
				}				
				if(!params[0].equals(INF)){
					tempDis = Integer.parseInt(params[0]);
					if(tempDis < min){
						min = tempDis;
						path = params[1];
					}
				}
				System.out.println("Now neighbor is "+neighbor);
			}
			if(min == Integer.MAX_VALUE){
				dist = INF;
			}else{
				dist = String.valueOf(min);
			}
			record = dist+","+path+","+neighbor;
			context.write(key, new Text(record));
			
		};
	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
				
		
		String source = "1";
		String[] params = null;
		String key, dist, path, neighbor;
		
		// Set configuration, use '-' to split key and value from text input file
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "-");
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "wikipath");
		
		job.setJarByClass(shortest_path.class);
		
		//Initialize file by the source node, 
		//source node distance  = 0
		//rest is infinitity  
		
		// Read original file, and create a temp file
		// that contain extra info, such as distance and path
		FileReader fileReader = new FileReader(INPUT_FILE);
		BufferedReader buffer = new BufferedReader(fileReader);
		
		//Make sure new file is written to HDFS system
		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path tempPath = new Path(TEMP_FILE);
		
		//set mapper output type
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);	
		
		//set reducer output type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
				
				
		if(fs.exists(tempPath)){
			fs.delete(tempPath, true);
		}
		FSDataOutputStream fout = fs.create(tempPath);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fout));
		
		String line;
		while((line=buffer.readLine()) != null){
			params = line.split(":");
			key = params[0];
			if(key.equals(source)){
				dist = "0";
			}else{
				dist = INF;
			}
			path = ":";
			neighbor = params[1];
			writer.write(key+"-"+dist+","+path+","+neighbor+"\n");
		}
		writer.close();
		fileReader.close();	
		FileInputFormat.addInputPath(job, tempPath);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		int index = 0;
		Text  tempkey = new Text();
		Text tempvalue = new Text();
		SequenceFile.Reader reader = new SequenceFile.Reader(job.getConfiguration(), SequenceFile.Reader.file(new Path(args[1]+"/part-r-00000")));
		while (reader.next( tempkey, tempvalue)){
			System.out.println( tempkey.toString() +" : "+tempvalue.toString() );
			
		}
		
		while (index<=5){
			
			job = new Job(conf, "wikipath");//new job each time.
			
			job.setJarByClass(shortest_path.class);
			
			//Initialize file by the source node, 
			//source node distance  = 0
			//rest is infinitity  
			
			// Read original file, and create a temp file
			// that contain extra info, such as distance and path
			fileReader = new FileReader(INPUT_FILE);
			buffer = new BufferedReader(fileReader);
			
			//Make sure new file is written to HDFS system
			//fs = FileSystem.get(job.getConfiguration());
			//tempPath = new Path(TEMP_FILE);
			
			//set mapper output type
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);	
			
			//set reducer output type
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			if ( index == 0){
				FileInputFormat.addInputPath(job, new Path(args[1] + "/part-r-00000"));
			}
			else{FileInputFormat.addInputPath(job, new Path(args[1] + Integer.toString(index-1)));}
			
			FileOutputFormat.setOutputPath(job, new Path(args[1] + Integer.toString(index) ));
			job.waitForCompletion(true);
			
			// output the current round of result
			reader = new SequenceFile.Reader(job.getConfiguration(), SequenceFile.Reader.file(new Path(args[1]+"/part-r-00000")));
			while (reader.next( tempkey, tempvalue)){
				System.out.println( tempkey.toString() +" : "+tempvalue.toString() );
				
			}
			
			index = index+1;
		}
		reader.close();
		
		
		//job.waitForCompletion(true);
		
	}

}
