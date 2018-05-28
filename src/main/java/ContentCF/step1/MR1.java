package ContentCF.step1;

import DAO.HdfsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * MapReduce 计算矩阵乘法
 * 将评分矩阵转置
 * 输入：步骤1的输出
 * 输出：用户ID(行) --- 物品ID(列) ---分值
 */

public class MR1
{
    //hdfs地址
    private static final String HDFS = "hdfs://master:9000";

    //输入文件的路径
    private static String inPath = "src/main/data/input/ContentCF/step1/";

    //输出文件的路径
    private static String outPath = "src/main/data/input/ContentCF/step1/";

    //输入文件名
    private static String fileName = "ItemProfile.txt";

    public int run()
    {

        try
        {
            Configuration conf = new Configuration();
            conf.set("mapreduce.app-submission.cross-platform", "true");
            conf.set("mapred.jar", "target/WordC-1.0-SNAPSHOT-jar-with-dependencies.jar");

            Job job = Job.getInstance(conf, "step1");
            job.setJarByClass(MR1.class);
            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reduce1.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            HdfsDAO dao = new HdfsDAO(HDFS, conf);
            String hdfsFilePath = "/boy/ContentCF/step1/";
            dao.mkdirs(hdfsFilePath);
            dao.copyFile(inPath + fileName, hdfsFilePath);

            FileInputFormat.addInputPath(job, new Path(hdfsFilePath + fileName));
            FileOutputFormat.setOutputPath(job, new Path(hdfsFilePath + "output"));

            boolean flag = job.waitForCompletion(true) == true;
            if (flag == true)
            {
                dao.download(hdfsFilePath + "output", outPath);
                return 1;
            }
            else
            {
                return -1;
            }

        } catch (IOException e)
        {
            e.printStackTrace();
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        } catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }

        return -1;
    }


    public static void main(String[] args)
    {
        int result = new MR1().run();
        if (result == 1)
            System.out.println("ok...");
        else if (result == -1)
            System.out.println("wrong...");
    }

}