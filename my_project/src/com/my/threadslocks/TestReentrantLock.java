package com.my.threadslocks;

import java.util.concurrent.locks.ReentrantLock;

public class TestReentrantLock {

	public static void main(String[] args) throws InterruptedException {

		final ReentrantLock l1 = new ReentrantLock();
		final ReentrantLock l2 = new ReentrantLock();

		Thread t1 = new Thread() {
			public void run() {
				try {
					l1.lockInterruptibly();
					System.out.println(Thread.currentThread().getName()+" get lock1");
					l1.lockInterruptibly();
					Thread.sleep(1000);
//					l2.lockInterruptibly();
					System.out.println(Thread.currentThread().getName()+" get lock2");
				} catch (InterruptedException e) {
					System.out.println("t1 interrupted");
				}
			}
		};

		Thread t2 = new Thread() {
			public void run() {
				try {
					l2.lockInterruptibly();
					System.out.println(Thread.currentThread().getName()+" get lock2");
					Thread.sleep(1000);
					l1.lockInterruptibly();
					System.out.println(Thread.currentThread().getName()+" get lock1");
				} catch (InterruptedException e) {
					System.out.println("t2 interrupted");
				}
			}
		};

		t1.start();
		t2.start();
		Thread.sleep(2000);
//		t1.interrupt();
//		t2.interrupt();
		t1.join();
		t2.join();
	}
}
