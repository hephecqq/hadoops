package com.wjl.rmi;

import java.net.ConnectException;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * 参考网上资料使用ZK动态监听RMI服务 
 */
public class Client {
	
	public static void main(String[] args) throws Exception {
		ZkCustomer zkCustomer = new ZkCustomer();
		while(true){
			HelloService helloService = zkCustomer.lookup();
			System.out.println(helloService.sayHello("Xixi"));
			TimeUnit.SECONDS.sleep(3);
		}
	}

}

class ZkCustomer{
	
	private ZooKeeper zk;
	private final static String ZNODE = "/registry";
	private final static String ZKSERVER = "CentosA:2181";
	private final static int TIMEOUT = 30000; 
	private CountDownLatch latch = new CountDownLatch(1);
	private volatile List<String> urls = new ArrayList<String>(); 
	
	public ZkCustomer() {
		try {
			zk = new ZooKeeper(ZKSERVER, TIMEOUT, new Watcher() {
				public void process(WatchedEvent event) {
					if (event.getState() == Event.KeeperState.SyncConnected) {
						latch.countDown();
					}
				}	
			});
			latch.await();
			
			if (zk != null) {
	            watchNode();
	        }
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	private void watchNode() {
		try {
            List<String> nodeList = zk.getChildren(ZNODE, new Watcher() {
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeChildrenChanged) {
                        watchNode(); //重新获取最新子节点中的数据
                    }
                }
            });
            List<String> dataList = new ArrayList<>();
            for (String node : nodeList) {
                byte[] data = zk.getData(ZNODE + "/" + node, false, null);
                dataList.add(new String(data));
            }
            urls = dataList;
        } catch (Exception e) {
        	e.printStackTrace();
        }
	}

	public HelloService lookup(){
		HelloService service = null;
		if(urls.size() > 0){
			String url ;
			if(urls.size() == 1){
				url = urls.get(0);
			}else{
				url = urls.get(ThreadLocalRandom.current().nextInt(urls.size()));
			}
			service = lookupService(url);
		}
		return service;
	}
	
	private HelloService lookupService(String url) {
		HelloService remote = null;
        try {
            remote = (HelloService)Naming.lookup(url);
        } catch (Exception e) {
            if (e instanceof ConnectException) {
                if (urls.size() != 0) {
                    url = urls.get(0);
                    return lookupService(url);
                }
            }
        }
        return remote;
    }
	
}

