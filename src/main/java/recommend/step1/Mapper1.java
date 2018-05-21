package recommend.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text>
{

    private Text outKey = new Text();
    private Text outValue = new Text();


    /*用户 物品 行为权重
      A,1,1
      C,3,5
      B,2,3
      B,5,3

      输出：物品,用户_分数
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] values = value.toString().split(",");
        String userID = values[0];
        String itemID = values[1];
        String score = values[2];

        outKey.set(itemID);
        outValue.set(userID + "_" + score);
        context.write(outKey, outValue);

    }
}
