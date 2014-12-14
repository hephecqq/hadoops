package com.wjl.hello;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * zk的api测测简单试试，类似Linux系统结构。
 * 
 * @author Jarvis.Wu
 *
 */
public class Hello {

	private final static String ZKSERVER = "CentosA:2181";
	private final static int TIMEOUT = 30000;

	public static void main(String[] args) throws Exception {
		ZooKeeper zk = new ZooKeeper(ZKSERVER, TIMEOUT, new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println("触发了事件类型：" + event.getType() + " 对应路径："
						+ event.getPath());
			}
		});

		// 创建几个节点 CreateMode.PERSISTENT 持久节点 ， 注意瞬时节点下不能创建子节点
		zk.create("/sClass", "终极一班".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		zk.create("/sClass/xiaoMi", "小米".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		zk.create("/sClass/xiaoLei", "小雷".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);

		// 遍历
		for (String child : zk.getChildren("/sClass", true)) {
			System.out.println("存在子目录：" + child + " 它的数据为："
					+ new String(zk.getData("/sClass/" + child, true, null)));
		}

		// 删除子目录节点
		zk.delete("/sClass/xiaoMi", -1);
		zk.delete("/sClass/xiaoLei", -1);

		// 删除父目录节点
		zk.delete("/sClass", -1);
		// 关闭连接
		zk.close();
	}

}
