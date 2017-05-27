package com.my.diningphilosophers;

import java.util.Random;

public class Philosopher extends Thread {

	private Chopstick left, right;
	private Random random;

	public Philosopher(Chopstick left, Chopstick right) {
		this.left = left;
		this.right = right;
		random = new Random();
	}

	public void run() {
		try {
			while (true) {
				Thread.sleep(random.nextInt(200)); // 思考一段时间
				System.out.println(Thread.currentThread().getName()+" 思考完毕，准备进餐...");
				synchronized (left) { // 拿起筷子1
					System.out.println(Thread.currentThread().getName()+" get left chopstick.");
					synchronized (right) { // 拿起筷子2
						System.out.println(Thread.currentThread().getName()+" get right chopstick.");
						Thread.sleep(random.nextInt(20000)); // 进餐一段时间
					}
				}
				System.out.println(Thread.currentThread().getName()+" 进餐完毕，归还筷子。");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
