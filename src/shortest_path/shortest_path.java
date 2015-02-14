package shortest_path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class shortest_path {

	static final String INF = "INF";
	static final String NA = "N/A";
	
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
			System.out.println("DEBUG: Mapper key="+key.toString()+" value="+value.toString());
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
			
		}
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
			
			for(Text page: pages){
				params = page.toString().split(",");
				if(!params[2].equals(NA)){
					neighbor = params[2];
				}				
				if(!params[0].equals(INF)){
					tempDis = Integer.parseInt(params[0]);
					if(tempDis < min){
						min = tempDis;
						path = params[1];
					}
				}
				
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
	public static void main(String[] args) throws Exception{
		String O_INPUT_FILE_PATH = "";// initial input path
		String INNER_FILE_PATH = "";
		String F_OUTPUT_FILE_PATH = "";//final output path
		int MAX_ITER = 7;
		
		Configuration conf =  new Configuration();
		Job job = Job.getInstance(conf, "PageRank");
		job.setJarByClass(shortest_path.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		//get file path from arguments
		O_INPUT_FILE_PATH = args[0];
		F_OUTPUT_FILE_PATH = args[1];
		INNER_FILE_PATH = args[2];
		
		//set initial file path
		FileInputFormat.setInputPaths(job, new Path(INNER_FILE_PATH + "00"));
		FileOutputFormat.setOutputPath(job, new Path(INNER_FILE_PATH + "0"));//1st inner path
		
		//prepare input file reader
		FileReader fileReader = new FileReader(O_INPUT_FILE_PATH);
		BufferedReader input_buffer = new BufferedReader(fileReader);
		
		SequenceFile.Writer writer = SequenceFile.createWriter(conf,
											SequenceFile.Writer.file(new Path(INNER_FILE_PATH + "00")), 
											SequenceFile.Writer.keyClass(Text.class),
											SequenceFile.Writer.valueClass(Text.class));
		String line;
		String[] items = null;
		String neighbors;
		String key ="";
		String dist="";
		String path ="";
		String source = args[3];
		String destination = args[4];
		while((line=input_buffer.readLine()) != null){
			items = line.split(":");
			key = items[0];
			if(key.equals(source)){
				dist = "0";
			}else{
				dist = INF;
			}
			path = ":";
			neighbors = items[1];
			writer.append(new Text(key), new Text(dist +","+ path + "," + neighbors));
		}
		writer.close();
		fileReader.close();
		input_buffer.close();
		job.waitForCompletion(true);//1st job
		
		for(int i= 1;i<=MAX_ITER;i++){
			job = Job.getInstance(conf);
			job.setJarByClass(shortest_path.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			FileInputFormat.setInputPaths(job, new Path(INNER_FILE_PATH + Integer.toString(i-1)));//read from and write to FORMAT_PATH
			if (i==MAX_ITER)//final output path
			{
				FileOutputFormat.setOutputPath(job, new Path(F_OUTPUT_FILE_PATH));
			}
			else //inner loop path
			{
				FileOutputFormat.setOutputPath(job, new Path(INNER_FILE_PATH + Integer.toString(i)));
			}
			
			job.waitForCompletion(true);//job iterations
		}
		
		
		
	}
	

}
