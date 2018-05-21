package recommend.step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Reduce1 extends Reducer<Text, Text, Text, Text>
{
    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * @param key     物品
     * @param values  用户_分数,用户_分数,用户_分数,用户_分数...
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        String itemId = key.toString();

        //<userID,score>
        Map<String, Integer> map = new HashMap<String, Integer>();


        //统计同一个商品中不同用户的分数 放入map中
        for (Text val : values)
        {
            String userId = val.toString().split("_")[0];
            String score = val.toString().split("_")[1];

            if (map.get(userId) == null)
                map.put(userId, Integer.valueOf(score));
            else
            {
                Integer preScore = map.get(userId);
                map.put(userId, Integer.valueOf(score) + preScore);
            }
        }

        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, Integer> entry : map.entrySet())
        {
            String udrtID = entry.getKey();
            String score = String.valueOf(entry.getValue());
            stringBuilder.append(udrtID + "_" + score + ",");
        }
        //去掉行末的","
        String line = null;
        if (stringBuilder.toString().endsWith(","))
        {
            line = stringBuilder.substring(0, stringBuilder.length() - 1);
        }

        if (itemId != null || itemId != "")
        {
            outKey.set(itemId);
            outValue.set(line);
            context.write(outKey, outValue);
        }

    }


}
