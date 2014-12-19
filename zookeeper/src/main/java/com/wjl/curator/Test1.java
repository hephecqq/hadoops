package com.wjl.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * curator轻松操作ZK的api功能。
 * 
 * @author Jarvis.Wu
 *
 */
public class Test1 {

	private final static String ZKNODE = "/wu";
	private final static String UTF8 = "UTF-8";
	private final static String ZKSERVER = "CentosA:2181";

	public static void main(String[] args) throws Exception {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);//重试策略
		CuratorFramework client = CuratorFrameworkFactory.newClient(ZKSERVER, retryPolicy);
		//还可以使用构建器添加更多参数设置
//		Builder builder = CuratorFrameworkFactory.builder().connectString(ZKSERVER)  
//                .connectionTimeoutMs(5000)  
//                .sessionTimeoutMs(5000)  
//                .retryPolicy(retryPolicy);  
//        builder.namespace(nameSpace);  
//        CuratorFramework client = builder.build();
		
		client.start();
		
		if(client.checkExists().forPath(ZKNODE)==null)
			System.out.println("路径" + ZKNODE + "不存在!");  
		
		client.create().forPath(ZKNODE, "Jarvis.Wu".getBytes());
		
		if(client.checkExists().forPath(ZKNODE)!=null)
			System.out.println("路径" + ZKNODE + "已经存在!");
		
		System.out.println("节点" + ZKNODE + "的数据为" + new String(client.getData().forPath(ZKNODE), UTF8));
		
		client.setData().forPath(ZKNODE, "Miss.Chen".getBytes());
		
		System.out.println("更新后，节点" + ZKNODE + "的数据为" + new String(client.getData().forPath(ZKNODE), UTF8));
		
		//client.delete().forPath(ZKNODE) zk不能删除删除有子节点的节点。 Curator可以。像下面这样链式删除就行了。
		client.delete().guaranteed().deletingChildrenIfNeeded().forPath(ZKNODE);
		
		client.close();
		
	}
}
