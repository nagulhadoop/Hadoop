package wordcount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver 
	{
		
public class MyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
			String line = value.toString();
			String lineParts[] = line.split(" ");
			for(String SingleWord : lineParts)
			{
				Text OutKey = new Text(SingleWord);
				IntWritable One = new IntWritable(1);
				context.write(OutKey, One);				
			}
		}
	}

		public class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		String mykey = key.toString().toUpperCase();
		
		context.write(new Text(mykey), new IntWritable(sum)); 
	}
}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Map Reduce Word Count");
		job.setJarByClass(MyDriver.class);
		job.setMapperClass(MyMap.class); 	
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(4);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
