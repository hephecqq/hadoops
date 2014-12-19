package com.wjl.curator;

import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * 通过curator轻松使用ZK的分布式锁。
 * 可以在main方法同时启动AB两个线程，这样是同一虚拟机。
 * 如果通过两次main方法分别启动A或B线程，不同虚拟机，更加分布式。
 * 
 * @author Jarvis.Wu
 *
 */
public class Test2 {
	
	public static void main(String[] args) {
		new TT("A线程").start();
		//new TT("B线程").start();
	}
	
}

class TT extends Thread{
	
	private final static String LOCKNODE = "/lockw";
	private final static String ZKSERVER = "CentosA:2181";
	private RetryPolicy retryPolicy = null;
	private CuratorFramework client = null;
	
	public TT(String name){
		super(name);
		this.retryPolicy = new ExponentialBackoffRetry(1000, 3);//重试策略
		this.client = CuratorFrameworkFactory.newClient(ZKSERVER, retryPolicy);
		this.client.start();  //打开客户端
	}
	
	@Override
	public void run() {
		InterProcessMutex lock = new InterProcessMutex(client, LOCKNODE);
		try {
			System.out.println(Thread.currentThread().getName() + " 准备获取锁!");
			if (lock.acquire(3, TimeUnit.MINUTES)) //设置3分钟获取不到锁就放弃，避免死锁 
			{
			    try 
			    {
			    	System.out.println(Thread.currentThread().getName() + " 正在占用锁!");
			    	TimeUnit.SECONDS.sleep(3); //占用3秒
			    }
			    finally
			    {
			        lock.release();
			    }
			    System.out.println(Thread.currentThread().getName() + " 已经释放锁!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.client.close(); //关闭客户端
	}
	
}
