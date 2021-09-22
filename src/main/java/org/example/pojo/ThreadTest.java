package org.example.pojo;

import java.util.LinkedList;

public class ThreadTest {
	
	public static void main(String[] args) {
		LinkedList<LittleBool> list = new LinkedList<LittleBool>();
		list.add(new LittleBool(true));
		list.add(new LittleBool(true));
		list.add(new LittleBool(true));
		LinkedList<Thread> t_list = new LinkedList<Thread>();
		for (int i = 0; i < 3; i++) {
			MyThread thread = new MyThread(list.get(i));
			Thread t = new Thread(thread);
			t_list.add(t);
			t.start();
		}
		/*
		for (int i = 0; i < 3; i++) {
			Thread t = t_list.get(i);
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}*/
		for (int i = 0; i < 3; i++) {
			System.out.println(list.get(i).getB());
		}
	}
	
}

class LittleBool{
	Boolean b;
	public LittleBool(Boolean b) {
		this.b = b;
	}
	public void setB(Boolean b) {
		this.b = b;
	}
	public Boolean getB() {
		return b;
	}
}

class MyThread implements Runnable{
	
	LittleBool b;
	
	public MyThread(LittleBool b) {
		this.b = b;
	}
	
	public void run() {
		b.setB(false);
	}
	
}
