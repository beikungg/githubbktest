package com.beikun.hdfs;
//注意这些都是hadoop的
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsCilent {
//客户端代码常用套路！！！
//1、获取一个客户端对象，这是在before里面的事情
//2、执行相关操作命令，这个可以卸载test里面
//3、关闭资源，这是after下面的

    private FileSystem fs;
    @Before
    public void init() throws URISyntaxException,IOException,InterruptedException{
        //指定uri,通信地址
        URI uri = new URI("hdfs://hadoop102:8020");
        //需要加的配置文件
        Configuration configuration = new Configuration();
        //如果不加用户，则会没有权限
        String user ="beikun";
        //获取到一个客户端对象
        fs = FileSystem.get(uri,configuration,user);
    }

    @Test
    public void testmkdir() throws IOException {
    //执行fs命令,创建目录
    fs.mkdirs(new Path("/xiyou/huaguoshan"));
    }
    @Test
    public void testPut() throws IOException {
    //从本地上传文件，没有这个就没有put方法，只有copyFromLocalFile
    //参数1、是否在本地删除数据 参数2、是否允许云端文件覆盖 3、本地路径 4、云端路径
    fs.copyFromLocalFile(false,true,new Path("D:\\sunwukong.txt"),
            new Path("hdfs://hadoop102/xiyou/huaguoshan"));
    }
    @Test
    public void testGet() throws IOException{
        //从本地上传文件，没有这个就没有get方法，只有copyToLocalFile
        //参数1、是否在本地删除数据 参数2、云端路径 参数3、本地路径 参数4、是否进行校验，通常false
        fs.copyToLocalFile(true,new Path("hdfs://hadoop102/xiyou/huaguoshan/sunwukong.txt"),new Path("D:\\"),false);
    }
    @Test
    public void testRm() throws IOException{
        //fs.delete删除命令
        //参数1、删除的远程路径 参数2、是否递归删除
        //删除文件(注意前缀hdfs://hadoop102可以不要)
        fs.delete(new Path("/xiyou/huaguoshan/sunwukong.txt"),false);
        //删除空目录
        fs.delete(new Path("/xiyou"),true);
        //删除非空目录
        fs.delete(new Path("/jinguo"),false);
    }
    @Test
    public void testmv() throws IOException{
        //fs.rename 在java里面，hadoop把重命名和移动这些操作集合起来了，统一为rename
        //参数1、原文件路径 参数2、目标文件路径
        //对文件修改名字
        fs.rename(new Path("/input/word.txt"),new Path("/input/ss.txt"));
        //文件移动并更改名字
        fs.rename(new Path("/input/ss.txt"),new Path("/cls.txt"));
        //目录更改名字
        fs.rename(new Path("/jinguo"),new Path("/chuguo"));
    }

    @Test
    public void filedetail() throws IOException{
    RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
    //输出文件信息
    while (listFiles.hasNext()) {
        LocatedFileStatus fileStatus = listFiles.next();

        System.out.println("========" + fileStatus.getPath() + "=========");
        System.out.println(fileStatus.getPermission());
        System.out.println(fileStatus.getOwner());
        System.out.println(fileStatus.getGroup());
        System.out.println(fileStatus.getLen());
        System.out.println(fileStatus.getModificationTime());
        System.out.println(fileStatus.getReplication());
        System.out.println(fileStatus.getBlockSize());
        System.out.println(fileStatus.getPath().getName());

        // 获取块信息
        BlockLocation[] blockLocations = fileStatus.getBlockLocations();
        System.out.println(Arrays.toString(blockLocations));
        }
    }

    @Test
    public void isFile() throws IOException{
    //判断是文件还是文件夹
    FileStatus[] listStatus = fs.listStatus(new Path("/"));

    for (FileStatus fileStatus : listStatus) {

        // 如果是文件
        if (fileStatus.isFile()) {
            System.out.println("f:"+fileStatus.getPath().getName());
        }else {
            System.out.println("d:"+fileStatus.getPath().getName());
        }
    }

    }




    @After
    public void close() throws  IOException{
        fs.close();
    }

}
