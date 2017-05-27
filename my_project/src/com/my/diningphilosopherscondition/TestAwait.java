package com.my.diningphilosopherscondition;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TestAwait {

	public static void main(String[] args) throws InterruptedException {
		ReentrantLock lock = new ReentrantLock();
		Condition condition = lock.newCondition();
		lock.lock();
		System.out.println(Thread.currentThread().getName() + " - " + lock.isLocked());
		TestLock t1 = new TestLock(lock);
		t1.start();
		TestCondition t2 = new TestCondition(lock, condition);
		t2.start();
		condition.await();
		System.out.println(Thread.currentThread().getName() + " - " + "end.");
	}

}

class TestCondition extends Thread {
	Condition condition;
	ReentrantLock lock;

	public TestCondition() {
	};

	public TestCondition(ReentrantLock lock, Condition condition) {
		this.condition = condition;
		this.lock = lock;
	};

	@SuppressWarnings("static-access")
	public void run() {
		try {
			currentThread().sleep(2000);
		} catch (InterruptedException e) {
		}
		lock.lock();
		condition.signal();
		lock.unlock();
		System.out.println(Thread.currentThread().getName() + " - " + "condition is signal.");
	}
}

class TestLock extends Thread {
	ReentrantLock lock;

	public TestLock() {
	};

	public TestLock(ReentrantLock lock) {
		this.lock = lock;
	};

	@SuppressWarnings("static-access")
	public void run() {
		try {
			Thread.currentThread().sleep(1000);
		} catch (InterruptedException e) {
		}
		System.out.println(Thread.currentThread().getName() + " - " + lock.isLocked());
		lock.lock();
		System.out.println(Thread.currentThread().getName() + " - " + lock.isLocked());
		lock.unlock();
	}
}