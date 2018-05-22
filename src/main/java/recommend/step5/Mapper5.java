package recommend.step5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Mapper5 extends Mapper<LongWritable, Text, Text, Text>
{

    private Text outKey = new Text();
    private Text outValue = new Text();

    private List<String> cacheList = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        super.setup(context);

        FileReader fr = new FileReader("itemUserScore3");
        BufferedReader br = new BufferedReader(fr);

        String line = null;
        while ((line = br.readLine()) != null)
        {
            cacheList.add(line);
        }

        br.close();
        fr.close();
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        //value  ： A_9.87,B_2.38,C_23.90
        String item_matrix1 = value.toString().split("\t")[0];
        String[] user_score_array_matrix1 = value.toString().split("\t")[1].split(",");

        //遍历全局缓存（步骤1的输出）
        for (String line : cacheList)
        {
            String item_matrix2 = line.toString().split("\t")[0];
            String[] user_score_array_matrix2 = line.toString().split("\t")[1].split(",");

            //如果行首的物品ID相同
            if (item_matrix1.equals(item_matrix2))
            {
                //遍历matrix1的列  A_9.87,B_2.38,C_23.90
                for (String user_score_matrix1 : user_score_array_matrix1)
                {
                    boolean flag = false;
                    String user_matrix1 = user_score_matrix1.split("_")[0];
                    String score_matrix1 = user_score_matrix1.split("_")[1];

                    //遍历matrix2的列  A_2,C_5
                    for (String user_score_matrix2 : user_score_array_matrix2)
                    {
                        //将用户已经有用过行为的商品评分置0
                        String user_matrix2 = user_score_matrix2.split("_")[0];
                        if (user_matrix1.equals(user_matrix2))
                        {
                            //相等 则不输出
                            flag = true;
                        }
                    }

                    //备注：这样比较感觉太罗嗦了，既然评分矩阵是6*3  推荐列表是6*3 而且输出结果会进行排序
                    //则位置肯定是对应的，直接可以一次循环就可以搞定。

                    //falg == false 表示这个用户还没有对这个物品产生过行为，则输出到最终的推荐列表
                    if (false == flag)
                    {
                        outKey.set(user_matrix1);
                        outValue.set(item_matrix1 + "_" + score_matrix1);
                        context.write(outKey, outValue);
                    }
                }
            }
        }

    }
}
