package UserCF.step5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce5 extends Reducer<Text, Text, Text, Text>
{
    private Text outKey = new Text();
    private Text outValue = new Text();


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {

        StringBuilder stringBuilder = new StringBuilder();
        for (Text value : values)
        {
            stringBuilder.append(value + ",");
        }

        String line = null;
        if (stringBuilder.toString().endsWith(","))
        {
            line = stringBuilder.substring(0, stringBuilder.length() - 1);
        }

        outKey.set(key);
        outValue.set(line);
        context.write(outKey, outValue);
    }
}
