package UserCF.step2;

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
 * 利用评分矩阵，构建用户与用户的相似度矩阵
 * 输入：步骤1的输出
 * 缓存：步骤1的输出
 * 输出：用户ID(行) --- 用户ID(列) ---相似度
 */
public class MR2
{

    //hdfs地址
    private static final String HDFS = "hdfs://master:9000";

    //输入文件的路径
    private static String inPath = "src/main/data/input/UserCF/step1/output/";

    //输出文件的路径
    private static String outPath = "src/main/data/input/UserCF/step2/";

    //输入文件名
    private static String fileName = "part-r-00000";

    //全局缓存文件路径
    private static String cachePath = "/boy/UserCF/step1/output/part-r-00000";

    public int run()
    {

        try
        {
            //System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.7.5"); //可选
            Configuration conf = new Configuration();
            //conf.set("fs.default.name", HDFS);//可选
            conf.set("mapreduce.app-submission.cross-platform", "true");
            //conf.set("mapreduce.framework.name", "yarn"); //可选
            conf.set("mapred.jar", "target/WordC-1.0-SNAPSHOT-jar-with-dependencies.jar");

            Job job = Job.getInstance(conf, "step2");

            //job.addCacheArchive(new URI(cachePath + "#itemUserScore"));
            URI uri = new URI(cachePath + "#itemUserScore");
            job.addCacheArchive(uri);

            job.setJarByClass(MR2.class);
            job.setMapperClass(Mapper2.class);
            job.setReducerClass(Reduce2.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            HdfsDAO dao = new HdfsDAO(HDFS, conf);
            String hdfsFilePath = "/boy/UserCF/step2/";
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
        } catch (URISyntaxException e)
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
        int result = new MR2().run();
        if (result == 1)
            System.out.println("ok...");
        else if (result == -1)
            System.out.println("wrong...");
    }

}
