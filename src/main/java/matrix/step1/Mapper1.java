package matrix.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class Mapper1 extends Mapper<LongWritable, Text, Text, Text>
{

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] datas = value.toString().split("\t");
        String row = datas[0];
        String[] lines = datas[1].split(",");
        for (int i = 0; i < lines.length; i++)
        {
            String column = lines[i].split("_")[0];
            String val = lines[i].split("_")[1];
            // 输出: key:列号  value:行号_值
            outKey.set(column);
            outValue.set(row + "_" + val);
            context.write(outKey, outValue);
        }
    }
}