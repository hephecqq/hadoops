package com.wjl.rmi;

import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 参考网上资料使用ZK动态监听RMI服务 
 */
public class Server {
	
	public static void main(String[] args) throws RemoteException {
		if(args.length < 2){
			System.out.println("请输入rmi绑定的host和port.");
		}
		String host = args[0];
		int port = Integer.valueOf(args[1]);
		HelloService service = new HelloServiceImpl(port);
		ZkProvider provider = new ZkProvider();
		provider.pulish(host, port, service);

		for(;;);
	}
	
}

class ZkProvider{
	
	private ZooKeeper zk;
	private final static String ZNODE = "/registry/provider";
	private final static String ZKSERVER = "CentosA:2181";
	private final static int TIMEOUT = 30000; 
	private CountDownLatch latch = new CountDownLatch(1);
	
	public ZkProvider() {
		try {
			zk = new ZooKeeper(ZKSERVER, TIMEOUT, new Watcher() {
				public void process(WatchedEvent event) {
					if (event.getState() == Event.KeeperState.SyncConnected) {
						latch.countDown();
					}
				}	
			});
			latch.await();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void pulish(String rmiHost, int rmiPort,Remote rmiRemote) {
		String url = pulishRMI(rmiHost, rmiPort, rmiRemote);
		if(url != null){
			providerZkNode(url);
		}
	}

	private String pulishRMI(String rmiHost, int rmiPort, Remote rmiRemote) {
		String url = null;
		try {
        	url = String.format("rmi://%s:%d/%s", rmiHost, rmiPort, rmiRemote.getClass().getName());
			LocateRegistry.createRegistry(rmiPort);
			Naming.rebind(url, rmiRemote);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
        System.out.println("publish rmi service (url: " + url);
		return url;
	}
	
	private void providerZkNode(String url) {
		try {
            byte[] data = url.getBytes();
            String path = zk.create(ZNODE, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL); // 创建一个临时性且有序的 ZNode
            System.out.println("create zookeeper node ("+path+" => "+url+")");
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
}