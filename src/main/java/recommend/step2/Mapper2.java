package recommend.step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper2 extends Mapper<LongWritable, Text, Text, Text>
{

    private Text outputKey = new Text();
    private Text outputValue = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        super.map(key, value, context);
    }
}
