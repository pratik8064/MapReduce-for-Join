import java.util.*;

import java.io.IOException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class equijoin {
	public static class Join_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			List<String> recordList = Arrays.asList(line.split(","));
			String joinKey = recordList.get(1);
			String record = "";
			for (int i = 0; i < recordList.size(); i++) {
				if (i == 0) {
					record += recordList.get(i);
					continue;
				}
				record += ", " + recordList.get(i);
			}
			output.collect(new Text(joinKey), new Text(record));
		}
	}

	public static class Join_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			int count = 0;
			boolean flag1 = false;
			boolean flag2 = false;
			String table1 = "";
			String table2 = "";

			Set<String> tableR = new LinkedHashSet<String>();
			Set<String> tableS = new LinkedHashSet<String>();
			Set<String> resultValues = new LinkedHashSet<String>();

			while(values.hasNext()){
				Text entry = values.next();
				String entryString = entry.toString();
				String[] splits = entryString.split(",");
				count += 1;

				if (!flag1) {
					table1 = splits[0].trim();
					flag1 = true;
				}
				if (flag1 && entryString.startsWith(table1)) {
					tableR.add(entryString);
					continue;
				}
				if (!flag2) {
					table2 = splits[0].trim();
					flag2 = true;
				}
				if (flag2 && entryString.startsWith(table2))
					tableS.add(entryString);
			}

			if (count == 1)
				return;

			for (String row1 : tableR) {
				for (String row2 : tableS) {
					output.collect(null, new Text(row1 + ", " + row2));
				}
			}
		}
	}

	public static void main(String args[]) throws Exception {
		JobConf conf = new JobConf(equijoin.class);
		conf.setJobName("EquiJoin");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Join_Mapper.class);
		conf.setReducerClass(Join_Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
