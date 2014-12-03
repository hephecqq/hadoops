package com.wjl.hello;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * HDFS简单操作。
 * 环境是： 虚拟机部署Hadoop伪分布式。版本1.2.1。另外，OS.A是我虚拟机的hostname。换成ip即可。
 * 
 * @author jl.wu
 *
 */
public class HelloHdfs {

	public static void main(String[] args) throws Exception {
		// upload();
		// download();
		// delete();
		// list();
		append();
		System.out.println("exec over!");
	}

	/**
	 * 上传文件到hdfs
	 * 
	 */
	public static void upload() throws FileNotFoundException, IOException {
		Configuration conf = new Configuration();

		String src = "D:/sample.txt";
		String dst = "hdfs://OS.A:9000/usr/local/hadoop/sample.txt"; // 
																		// /usr/local/hadoop是指定存放的路径?

		InputStream in = new BufferedInputStream(new FileInputStream(src));
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out = fs.create(new Path(dst));
		IOUtils.copyBytes(in, out, 4096, true);
		IOUtils.closeStream(out);
		IOUtils.closeStream(in);
	}

	/**
	 * 从hdfs下载文件
	 */
	public static void download() throws FileNotFoundException, IOException {
		Configuration conf = new Configuration();

		String dst = "hdfs://OS.A:9000/wjl";
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FSDataInputStream in = fs.open(new Path(dst));

		ByteArrayOutputStream out = new ByteArrayOutputStream();

		byte[] buf = new byte[1024];
		int len;
		while ((len = in.read(buf)) != -1) {
			out.write(buf, 0, len);
		}

		System.out.println(out.toString());
		IOUtils.closeStream(out);
		IOUtils.closeStream(in);
	}

	/**
	 * 删除文件
	 * 
	 */
	public static void delete() throws FileNotFoundException, IOException {
		Configuration conf = new Configuration();
		String dst = "hdfs://OS.A:9000/usr/local/hadoop/sample.txt";
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		fs.deleteOnExit(new Path(dst));
		fs.close();
	}

	/**
	 * 遍历hdfs文件
	 */
	public static void list() throws FileNotFoundException, IOException {
		Configuration conf = new Configuration();
		String dst = "hdfs://OS.A:9000/usr/local/hadoop";
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FileStatus fileList[] = fs.listStatus(new Path(dst));
		int size = fileList.length;
		for (int i = 0; i < size; i++) {
			if (fileList[i].isDir()) {
				System.out.println("文件夹：" + fileList[i].getPath().getName());
			} else {
				System.out.println("文件:" + fileList[i].getPath().getName() + " 大小:" + fileList[i].getLen());
			}
		}
		fs.close();
	}

	/**
	 * append内容到HDFS上的文件。 需要升级到hadoop 2.x版本 需要在hdfs-site.xml配置
	 * <property><name>dfs.support.append</name><value>true</value></property>
	 * 不然会报错：java.io.IOException: Append is not supported. Please see the
	 * dfs.support.append...
	 */
	public static void append() throws FileNotFoundException, IOException {
		Configuration conf = new Configuration();
		String dst = "hdfs://OS.A:9000/usr/local/hadoop/sample.txt";
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FSDataOutputStream out = fs.append(new Path(dst));
		String line = "增加一行：Hello HDFS!!";
		out.writeChars(line);
		out.close();
		fs.close();
	}

}