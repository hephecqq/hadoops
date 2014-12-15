package com.wjl.hello2;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 模拟使用zk分布式配置管理
 * 
 * @author Jarvis.Wu
 *
 */
public class Master {

	private final static String CONF_NODE = "/Mconf";

	private final static String ZKSERVER = "CentosA:2181";

	private final static int TIMEOUT = 30000;

	public void service() throws Exception {
		final ExecutorService executorService = Executors.newCachedThreadPool();

		ZooKeeper zk = new ZooKeeper(ZKSERVER, TIMEOUT, new Watcher() {
			public void process(WatchedEvent event) {
				if (event.getState() == Event.KeeperState.SyncConnected) {
					System.out.println("Master成功连接上Zookeeper服务器.");
				}
			}
		});

		Stat stat = zk.exists(CONF_NODE, false);
		if (stat == null) {
			zk.create(CONF_NODE, "uuid=e466c840-8eda-409c-b4a7-d9a1363a4acc".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		executorService.execute(new Secondary("app1"));
		executorService.execute(new Secondary("app2"));

		while (true) { // 每隔10秒刷新数据
			TimeUnit.SECONDS.sleep(10);
			zk.setData(CONF_NODE, ("uuid=" + UUID.randomUUID()).getBytes(), -1);
		}
	}

	class Secondary implements Runnable {

		private ZooKeeper zk;
		private boolean init;

		public Secondary(String name) throws Exception {
			Thread.currentThread().setName(name);
			zk = new ZooKeeper(ZKSERVER, TIMEOUT, new Watcher() {
				public void process(WatchedEvent event) {
					if (event.getState() == Event.KeeperState.SyncConnected && !init) {
						init = true;
						System.out.println("Secondary成功连接上Zookeeper服务器.当前数据：" + getData(CONF_NODE)); //获取并监控节点数据
					}
					
					if(event.getType()==Event.EventType.NodeDataChanged){  
						StringBuilder out = new StringBuilder();
						out.append(Thread.currentThread().getName());
						out.append("监听的节点\"");
						out.append(event.getPath());
						out.append("\"的数据发生改变了.");
						String path = event.getPath();
						String newData = getData(path);
						if (newData != null && !newData.equals(""))
							out.append("改变之后的数据：" + newData);
						System.out.println(out);
				    }
				}
			});
		}

		private String getData(String path) {
			try {
				return new String(zk.getData(path, true, null));
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		}

		public void run() {
			while (true)
				;
		}
	}

	public static void main(String[] args) throws Exception {
		new Master().service();
	}
}
