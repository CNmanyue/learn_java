package com.my.diningphilosopherscondition;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import java.util.Random;

class Philosopher extends Thread {

	private boolean eating;
	private Philosopher left;
	private Philosopher right;
	private ReentrantLock table;
	private Condition condition;
	private Random random;
	private int thinkCount;

	public Philosopher(ReentrantLock table) {
		eating = false;
		this.table = table;
		condition = table.newCondition();
		random = new Random();
	}

	public void setLeft(Philosopher left) {
		this.left = left;
	}

	public void setRight(Philosopher right) {
		this.right = right;
	}

	public void run() {
		try {
			while (true) {
				think();
				eat();
			}
		} catch (InterruptedException e) {
		}
	}

	private void think() throws InterruptedException {
		table.lock();
		try {
			eating = false;
			left.condition.signal();
			right.condition.signal();
		} finally {
			table.unlock();
		}
		++thinkCount;
		if (thinkCount % 10 == 0)
			System.out.println("Philosopher " + this + " has thought " + thinkCount + " times");
		Thread.sleep(random.nextInt(3000));
	}

	private void eat() throws InterruptedException {
		table.lock();
		try {
			while (left.eating || right.eating)
				condition.await();
			eating = true;
		} finally {
			table.unlock();
		}
		Thread.sleep(random.nextInt(3000));
	}

	public boolean isEating() {
		return eating;
	}

	public void setEating(boolean eating) {
		this.eating = eating;
	}
}
