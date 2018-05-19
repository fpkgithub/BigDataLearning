package WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;


/**
 * WordCount 统计
 */
public class WordCount2
{

    private static final String HDFS = "hdfs://master:9000";

    public static void main(String[] args) throws Exception
    {
        //如果没有这句话，会报找不到winutils.exe的错误，也不知道是不是由于我修改了环境变量之后没有重启的原因。
        System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.7.5");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", HDFS);
        //意思是跨平台提交，在windows下如果没有这句代码会报错 "/bin/bash: line 0: fg: no job control"，
        //去网上搜答案很多都说是linux和windows环境不同导致的一般都是修改YarnRunner.java，但是其实添加了这行代码就可以了。
        conf.set("mapreduce.app-submission.cross-platform", "true");
        //集群的方式运行，非本地运行。
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapred.jar", "D:\\IDE\\Idea\\BigDataLearning\\target\\WordC-1.0-SNAPSHOT-jar-with-dependencies.jar");

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //hdfs路径
        //String HDFS = "hdfs://localhost:8020";
        //判断输入路径并创建
        String local = "src/main/data/input/wordcount/words-file";
        String remote = "/boy/WordCount2";
        String fileName = local.substring(local.lastIndexOf("/") + 1, local.length());
        if (!isExistsFile(remote + fileName, conf))
        {
            mkdirs(remote, conf);
            putFile(local, remote, conf);
        } else
        {
            System.out.println(fileName + " already exist... ");
        }

        //FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/Test/wordcount.txt"));
        FileInputFormat.addInputPath(job, new Path(HDFS + remote));
        //FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/Test/output2"));
        FileOutputFormat.setOutputPath(job, new Path(HDFS + remote + "/output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }


    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    private static void putFile(String local, String remote, Configuration conf) throws IOException
    {
        FileSystem fs = FileSystem.get(URI.create(HDFS), conf);
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }

    private static boolean isExistsFile(String file, Configuration conf) throws IOException
    {

        FileSystem fs = FileSystem.get(URI.create(HDFS), conf);
        return fs.exists(new Path(file));
    }


    private static void mkdirs(String folder, Configuration conf) throws IOException
    {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(HDFS), conf);
        if (!fs.exists(path))
        {
            fs.mkdirs(path);
            System.out.println("Create: " + folder);
        }
        fs.close();
    }

    public static void getFile(String remote, String local, Configuration conf) throws IOException
    {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(HDFS), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);
        fs.close();
    }

}