package matrix.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * 慕课网
 * MapReduce 计算矩阵乘法
 */
public class MapperReduce2
{


    //hdfs路径
    static final String HDFS = "hdfs://localhost:8020";
    //当前项目绝对路径
    static String basePath = System.getProperty("user.dir");
    //输入文件目录
    static String inputPath = basePath + "/data/input/matrix/step2";
    //输出文件目录
    static String outPath = basePath + "/data/output/matrix/step2";
    //第一步的输出作为第二步的缓存目录
    static String cachePath = basePath + "/data/output/matrix/step1/part-r-00000";


    public static void main(String[] args) throws Exception
    {
        //实例化一个Job对象
        Configuration conf = new Configuration(); //加载配置文件
        Job job = Job.getInstance(conf, "step2");

        job.addCacheArchive(new URI(cachePath + "#mymatrix"));

        //设置Job作业所在jar包
        job.setJarByClass(MapperReduce2.class);

        //设置本次作业的Mapper类和Reducer类
        job.setMapperClass(Mapper2.class);
        job.setReducerClass(Reduce2.class);

        //设置Mapper类的输出key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置Reducer类的输出key-value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //指定本次作业要处理的原始文件所在路径（注意，是目录）
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        //指定本次作业产生的结果输出路径（也是目录）
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        //提交本次作业，并打印出详细信息
        job.waitForCompletion(true);
    }
}