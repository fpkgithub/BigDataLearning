package recommend.step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class Mapper2 extends Mapper<LongWritable, Text, Text, Text>
{

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    private List<String> cacheList = new ArrayList<String>();
    private DecimalFormat df = new DecimalFormat("0:00");

    /**
     * 缓存步骤1的输出
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        super.setup(context);

        FileReader fr = new FileReader("itemUserScore");
        BufferedReader br = new BufferedReader(fr);

        String line = null;
        while ((line = br.readLine()) != null)
        {
            cacheList.add(line);
        }

        br.close();
        fr.close();

    }

    int i = 1;

    /**
     * 左侧矩阵
     *
     * @param key     行
     * @param value   行 tab 列_值,列_值,列_值,列_值
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        //行
        String row_matrix1 = value.toString().split("\t")[0];
        //列_值
        String[] column_value_array_matrix1 = value.toString().split("\t")[1].split(",");


        //计算余弦相似度，需要计算左侧矩阵行空间距离
        double denominator1 = 0;
        for (String colunm_value : column_value_array_matrix1)
        {
            String score = colunm_value.split("_")[1];
            denominator1 += Double.valueOf(score) * Double.valueOf(score);
        }
        denominator1 = Math.sqrt(denominator1);

        //cacheList 右侧矩阵
        for (String line : cacheList)
        {
            //line 右侧矩阵的行 格式：行 tab 列_值,列_值,列_值,列_值
            String row_matrix2 = line.toString().split("\t")[0];
            String[] column_value_array_matrix2 = line.toString().split("\t")[1].split(",");

            //计算余弦相似度，需要计算右侧矩阵行空间距离
            double denominator2 = 0;
            for (String colunm_value : column_value_array_matrix2)
            {
                String score = colunm_value.split("_")[1];
                denominator2 += Double.valueOf(score) * Double.valueOf(score);
            }
            denominator2 = Math.sqrt(denominator2);

            //矩阵两行相乘得到的结果
            int numerator = 0;
            for (String column_value_matrix1 : column_value_array_matrix1)
            {
                //column_value_matrix1: A_2 C_5
                String column_matrix1 = column_value_matrix1.split("_")[0];
                String value_matrix1 = column_value_matrix1.split("_")[1];

                //遍历右矩阵每一行的每一列
                for (String colunm_value_matrix2 : column_value_array_matrix2)
                {
                    //colunm_value_matrix2: A_2 C_5  1 、  A_10 B_3   2 、
                    if (colunm_value_matrix2.startsWith(column_matrix1 + "_"))
                    {
                        //这里的if判断是保证对应的列值相乘
                        String value_matrix2 = colunm_value_matrix2.split("_")[1];
                        //对应列的值相乘累加
                        numerator += Integer.valueOf(value_matrix1) * Integer.valueOf(value_matrix2);

                    }
                }
            }

            System.out.println(i++ + " : " + numerator + " " + denominator1 + " " + denominator2);
            double cos = numerator / (denominator1 * denominator2);

            if (cos == 0)
            {
                continue;
            }

            outputKey.set(row_matrix1);
            outputValue.set(row_matrix2 + "_" + df.format(cos));
            //输出：行 列_值
            context.write(outputKey, outputValue);

        }

    }
}
