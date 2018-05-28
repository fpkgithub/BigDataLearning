package temperature;

import DAO.HdfsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RunJob
{

    private static final String HDFS = "hdfs://master:9000";

    private static String inputPath = "src/main/data/input/temperature/";
    private static String outputPath = "src/main/data/input/temperature/";
    private static String fileName = "year_temperature";


    private int run()
    {

        try
        {
            Configuration conf = new Configuration();
            conf.set("mapreduce.app-submission.cross-platform", "true");
            conf.set("mapred.jar", "target/WordC-1.0-SNAPSHOT-jar-with-dependencies.jar");

            Job job = Job.getInstance(conf, "Temperature");
            job.setJarByClass(RunJob.class);
            job.setMapperClass(MapperJob.class);
            job.setReducerClass(ReduceJob.class);

            job.setMapOutputKeyClass(MyKeyPair.class);
            job.setMapOutputValueClass(Text.class);

            job.setNumReduceTasks(4);
            job.setPartitionerClass(Partition.class);
            job.setSortComparatorClass(Sort.class);
            job.setGroupingComparatorClass(Group.class);


            HdfsDAO dao = new HdfsDAO(HDFS, conf);
            String hdfsFilePath = "/boy/temperature/";
            dao.mkdirs(hdfsFilePath);
            dao.copyFile(inputPath + fileName, hdfsFilePath);

            FileInputFormat.addInputPath(job, new Path(hdfsFilePath + fileName));
            FileOutputFormat.setOutputPath(job, new Path(hdfsFilePath + "output"));

            boolean flag = job.waitForCompletion(true);

            if (flag == true)
            {
                dao.download(hdfsFilePath + "output", outputPath);
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
        int result = new RunJob().run();
        if (result == 1)
            System.out.println("ok...");
        else if (result == -1)
            System.out.println("wrong...");

    }

}
