package DAO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.net.URI;

/**
 * HDFS的增删改查
 */
public class HdfsDAO
{


    //HDFS访问地址
    //private static final String HDFS = "hdfs://localhost:8020";
    private static final String HDFS = "hdfs://master:9000";

    private HdfsDAO(Configuration conf)
    {
        this(HDFS, conf);
    }

    public HdfsDAO(String hdfs, Configuration conf)
    {
        this.hdfsPath = hdfs;
        this.conf = conf;
    }

    //hdfs路径
    private String hdfsPath;
    //Hadoop系统配置
    private Configuration conf;

    //启动函数
    public static void main(String[] args) throws IOException
    {
        JobConf conf = config();
        HdfsDAO hdfs = new HdfsDAO(conf);
        //hdfs.mkdirs("/tmp/new/two");
        hdfs.ls("/");

        //        hdfs.mkdirs("/boy/love");
        //
        //        hdfs.ls("/");
        //
        //        hdfs.rm("/boy");
        //
        //        hdfs.ls("/");
    }

    //加载Hadoop配置文件
    private static JobConf config()
    {
        JobConf conf = new JobConf(HdfsDAO.class);
        conf.setJobName("other.HdfsDAO");
        return conf;
    }

    //API实现
    private void cat(String remoteFile) throws IOException
    {
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FSDataInputStream fsdis = null;
        System.out.println("cat: " + remoteFile);
        try
        {
            fsdis = fs.open(path);
            IOUtils.copyBytes(fsdis, System.out, 4096, false);
        } finally
        {
            IOUtils.closeStream(fsdis);
            fs.close();
        }
    }

    public void rm(String folder) throws IOException
    {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.deleteOnExit(path);
        System.out.println("Delete: " + folder);
        fs.close();
    }

    public void mkdirs(String folder) throws IOException
    {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        if (!fs.exists(path))
        {
            fs.mkdirs(path);
            System.out.println("Create: " + folder);
        }
        fs.close();
    }

    private void ls(String folder) throws IOException
    {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);

        System.out.println("ls: " + folder);
         /*
         FileStatus[] list = fs.listStatus(path);
         System.out.println("ls: " + folder);
         System.out.println("==========================================================");
         for (LocatedFileStatus f : list)
         {
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDir(), f.getLen());
         }
        */

        RemoteIterator<LocatedFileStatus> list = fs.listFiles(path, true);
        while (list.hasNext())
        {
            LocatedFileStatus status = list.next();
            System.out.printf("name: %s, folder: %s, size: %d\n", status.getPath(), status.isDirectory(), status.getLen());
        }
        System.out.println("==========================================================");
        fs.close();
    }

    public void copyFile(String local, String remote) throws IOException
    {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }

    public void download(String remote, String local) throws IOException
    {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);
        fs.close();
    }

    public void createFile(String file, String content) throws IOException
    {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try
        {
            os = fs.create(new Path(file));
            os.write(buff, 0, buff.length);
            System.out.println("Create: " + file);
        } finally
        {
            if (os != null)
                os.close();
        }
        fs.close();
    }
}
