package temperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceJob extends Reducer<MyKeyPair, Text, MyKeyPair, Text>
{


    /**
     * @param key     MyKeyPair(year,temperature)
     * @param values  temperature
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(MyKeyPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        for (Text value : values)
        {
            context.write(key, value);
        }
    }
}
