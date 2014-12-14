package com.wjl.rmi;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class HelloServiceImpl extends UnicastRemoteObject implements HelloService{

	private int port;
	private static final long serialVersionUID = -8963934780901383218L;
	
	public HelloServiceImpl(int port) throws RemoteException {
		this.port = port;
	}
	
	@Override
	public String sayHello(String name) {
		System.out.println("当前调用端口：" + port + "参数：" + name);
		return "Hello," + name;
	}

}
