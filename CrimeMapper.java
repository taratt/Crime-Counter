import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class CrimeMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final static int DISTRICT_COLUMN = 12;
    

    public void map(LongWritable key, Text value, OutputCollector <Text, IntWritable> output, Reporter reporter) throws IOException {

        String fileValue = value.toString();
	if(!fileValue.contains("District")){
        String[] crimeRecord = fileValue.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        output.collect(new Text(crimeRecord[DISTRICT_COLUMN]), one);
	}
    }
}