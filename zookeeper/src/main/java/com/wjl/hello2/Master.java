package com.wjl.hello2;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 模拟使用zk分布式配置管理
 * 
 * @author Jarvis.Wu
 *
 */
public class Master {

	private final static String ZNODE = "/conf";
	private final static String ZKSERVER = "CentosA:2181";
	private final static int TIMEOUT = 30000;

	public void service() throws Exception{
		final ExecutorService executorService = Executors.newCachedThreadPool();

		ZooKeeper zk = new ZooKeeper(ZKSERVER, TIMEOUT, new Watcher() {
			public void process(WatchedEvent event) {
				if (event.getState() == Event.KeeperState.SyncConnected) {
					System.out.println("成功连接上Zookeeper服务器.");
				}
			}
		});

		zk.create(ZNODE, "name=jl.wu".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL);

		executorService.execute(new Secondary("app1"));
		executorService.execute(new Secondary("app2"));
		
		Thread.sleep(1000);
		
		zk.setData(ZNODE, "age=20".getBytes(), -1);
		
		while(true)
			;
	}

	class Secondary implements Runnable {
		public Secondary(String name) throws Exception {
			Thread.currentThread().setName(name);
			ZooKeeper zk = new ZooKeeper(ZKSERVER, TIMEOUT, new Watcher() {
				public void process(WatchedEvent event) {
					if (event.getState() == Event.KeeperState.SyncConnected) {
						System.out.println("成功连接上Zookeeper服务器.");
					}
				}
			});
			zk.exists(ZNODE, new Watcher() {
				public void process(WatchedEvent event) {
					StringBuilder out = new StringBuilder();
					out.append(Thread.currentThread().getName());
					out.append("监听的节点\"");
					out.append(event.getPath());
					out.append("\"的数据发生改变了.");
					if(event.getType() == EventType.NodeDataChanged){
						String newData = null;
						try {
							newData = new String(zk.getData(event.getPath(), true, null));
						} catch (KeeperException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						if(newData != null && !newData.equals(""))
							out.append("改变之后的数据：" + newData);
					}
					System.out.println(out);
				}
			});
		}
		public void run() {
			while(true);
		}
	}
	
	public static void main(String[] args) throws Exception {
		new Master().service();
	}
}
