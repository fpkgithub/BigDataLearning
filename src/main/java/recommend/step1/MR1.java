package recommend.step1;


import DAO.HdfsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;


/**
 * 根据用户行为列表计算用户、物品的评分矩阵
 */

public class MR1
{

    //hdfs地址
    private static final String HDFS = "hdfs://master:9000";

    //输入文件的路径
    private static String inPath = "src/main/data/input/recommend/step1/input/";

    //输出文件的路径
    private static String outPath = "src/main/data/input/recommend/step1/";

    //输入文件名
    static String fileName = "ActionList";

    public int run()
    {
        try
        {
            System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.7.5");

            Configuration conf = new Configuration();
            conf.set("fs.default.name", HDFS);
            conf.set("mapreduce.app-submission.cross-platform", "true");
            conf.set("mapreduce.framework.name", "yarn");
            conf.set("mapred.jar", "D:\\IDE\\Idea\\BigDataLearning\\target\\WordC-1.0-SNAPSHOT-jar-with-dependencies.jar");

            Job job = Job.getInstance(conf, "step1");

            job.setJarByClass(MR1.class);

            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reduce1.class);


            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //输入文件设置
            //1: 创建上传的路径
            HdfsDAO dao = new HdfsDAO(HDFS, conf);
            String hdfsFilePath = "/boy/recommend/step1/";
            dao.mkdirs(hdfsFilePath);

            //2：上传文件到hdfs
            dao.copyFile(inPath + fileName, hdfsFilePath);

            //3: 指定本次作业要处理的原始文件所在路径
            FileInputFormat.addInputPath(job, new Path(hdfsFilePath + fileName));

            //输出路径
            FileOutputFormat.setOutputPath(job, new Path(hdfsFilePath + "output"));

            boolean flag = job.waitForCompletion(true);

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
