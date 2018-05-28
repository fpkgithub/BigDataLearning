package ContentCF.step2;

import DAO.HdfsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * 将评分矩阵转置
 * 输入：步骤2的输出
 * 缓存：步骤1的输出
 * 输出：用户ID(行) --- 物品ID(列) ---分值
 */

public class MR2
{
    //hdfs地址
    private static final String HDFS = "hdfs://master:9000";

    //输入文件的路径
    private static String inPath = "src/main/data/input/ContentCF/step2/";

    //输出文件的路径
    private static String outPath = "src/main/data/input/ContentCF/step2/";

    //输入文件名
    private static String fileName = "ItemUser.txt";

    //全局缓存文件路径
    private static String cachePath = "/boy/ContentCF/step1/output/part-r-00000";


    public int run()
    {

        try
        {
            Configuration conf = new Configuration();
            conf.set("mapreduce.app-submission.cross-platform", "true");
            conf.set("mapred.jar", "target/WordC-1.0-SNAPSHOT-jar-with-dependencies.jar");

            Job job = Job.getInstance(conf, "step2");

            //缓存：步骤3的输出
            URI uri = new URI(cachePath + "#SoreMatrixZhuanzhi");
            job.addCacheArchive(uri);

            job.setJarByClass(MR2.class);
            job.setMapperClass(Mapper2.class);
            job.setReducerClass(Reduce2.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            HdfsDAO dao = new HdfsDAO(HDFS, conf);
            String hdfsFilePath = "/boy/ContentCF/step2/";
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
        } catch (URISyntaxException e)
        {
            e.printStackTrace();
        }

        return -1;
    }


    public static void main(String[] args)
    {
        int result = new MR2().run();
        if (result == 1)
            System.out.println("ok...");
        else if (result == -1)
            System.out.println("wrong...");
    }

}