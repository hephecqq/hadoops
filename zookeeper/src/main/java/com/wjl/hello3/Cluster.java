package com.wjl.hello3;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class Cluster {
	
	private final static String ROOT = "/cluster";
	private final static String ZKSERVER = "CentosA:2181";
	private final static int TIMEOUT = 30000;
	
	//运行程序之前zk服务器 已经出了化一个/cluster节点
	public void service() throws Exception{
		final ExecutorService executorService = Executors.newCachedThreadPool();

		executorService.execute(new Secondary("server1", 0));
		executorService.execute(new Secondary("server2", 5));
		executorService.execute(new Secondary("server3", 0));
		executorService.execute(new Secondary("server4", 10));
		
		while(true)
			;
	}
	
	class Secondary implements Runnable{
		private int interrupt;
		private ZooKeeper zk;
		private String host;
		
		Secondary(String host, int interrupt) throws Exception{
			this.host = host;
			Thread.currentThread().setName(host);
			this.interrupt = interrupt;
			
			zk = new ZooKeeper(ZKSERVER, TIMEOUT, new Watcher() {
				public void process(WatchedEvent event) {
					if (event.getState() == Event.KeeperState.SyncConnected) {
						try {
							System.out.println("当前节点：" + zk.getChildren(ROOT, true));
						} catch (KeeperException | InterruptedException e) {
							e.printStackTrace();
						}
					}
				}	
			});
			
			zk.create(ROOT + "/" + host, host.getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL);
			
			zk.getChildren(ROOT, true);
		}
		@Override
		public void run() {
			if(interrupt == 0){
				while(true);  //interrupt=0的线程 
			}
			else{
				while(interrupt-- > 0){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			System.out.println(host + "线程结束，Session关闭了.");
			try {
				zk.delete(ROOT + "/" + host, -1);  //模拟节点断开 Session关闭
				zk.close();                        //模拟节点断开 Session关闭
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		new Cluster().service();
	}

}
