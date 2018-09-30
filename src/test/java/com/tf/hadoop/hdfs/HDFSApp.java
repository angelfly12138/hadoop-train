package com.tf.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * Hadoop HDFS Java API操作
 */
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://master:9000";

    FileSystem fileSystem = null;
    Configuration configuration = null;


    /**
     * 创建HDFS目录
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        output.write("hello hadoop".getBytes());
        output.flush();
        output.close();
    }

    /**
     * 查看HDFS文件的内容
     */
    @Test
    public void cat() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }

    /**
     * 重命名
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 上传文件到HDFS
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path localPath = new Path("C:/Users/dell-pc/Desktop/c.txt");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 上传文件到HDFS
     */
    @Test
    public void copyFromLocalFileWithProgress() throws Exception {
        InputStream in = new BufferedInputStream(
                new FileInputStream(
                        new File("C:/Users/dell-pc/Desktop/eclipse-linux-gtk-x86_64.tar.gz")));

        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/eclipse.tar.gz"),
                new Progressable() {
                    public void progress() {
                        System.out.print(".");  //带进度提醒信息
                    }
                });
        IOUtils.copyBytes(in, output, 4096);
    }

    /**
     * 下载HDFS文件
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path localPath = new Path("C:/Users/dell-pc/Desktop");
        Path hdfsPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.copyToLocalFile(hdfsPath, localPath);
//        String hdfs = "/hdfsapi/test/b.txt";
//        String local = "C:/Users/dell-pc/Desktop";
//        configuration.set("fs.defaultFs", HDFS_PATH);
//        FileSystem fs = FileSystem.get(URI.create(hdfs), configuration);
//        FSDataInputStream fsdi = fs.open(new Path(hdfs));
//        OutputStream output = new FileOutputStream(local);
//        IOUtils.copyBytes(fsdi, output, 4096, true);
    }

    /**
     * 查看某个目录下的所有文件
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));

        for (FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();

            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    /**
     * 删除
     */
    @Test
    public void delete() throws Exception {
        fileSystem.delete(new Path("/hdfsapi/test/c.txt"), true);
    }

    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSApp.setUp");
        configuration = new Configuration();
        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.5.2");
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "zkpk");
    }

    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;
        System.out.println("HDFSApp.tearDown");

    }
}
