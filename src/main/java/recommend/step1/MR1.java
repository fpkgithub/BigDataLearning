package recommend.step1;


import DAO.HdfsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 根据用户行为构建评分矩阵
 */

public class MR1
{

    //hdfs路径
    private static final String HDFS = "hdfs://master:9000";
    //当前项目绝对路径
    static String basePath = System.getProperty("user.dir");
    //输入文件的路径
    static String inputPath = "src/main/data/input/recommend/step1/ActionList";
    //文件名
    static String fileName = "ActionList";


    public static void main(String[] args) throws Exception
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
        HdfsDAO hdfsDAO = new HdfsDAO(HDFS, conf);
        String hdfsPath = "/boy/recommend/step/step1/";
        hdfsDAO.mkdirs(hdfsPath);

        //2：上传文件到hdfs
        hdfsDAO.copyFile(inputPath, hdfsPath);

        //3: 指定本次作业要处理的原始文件所在路径
        FileInputFormat.addInputPath(job, new Path(hdfsPath + fileName));

        //输出路径
        String outputPath = hdfsPath + "output";
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean flag = job.waitForCompletion(true);

        if (flag == true)
        {
            System.out.println("It's ok !");
        }
        else
        {
            System.out.println("have wrong...");
        }

    }

}
