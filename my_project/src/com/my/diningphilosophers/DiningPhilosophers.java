package com.my.diningphilosophers;

public class DiningPhilosophers {

	public static void main(String[] args) throws InterruptedException {
		Philosopher[] philosophers = new Philosopher[5];
		Chopstick[] chopsticks = new Chopstick[5];

		for (int i = 0; i < 5; ++i)
			chopsticks[i] = new Chopstick(i);
		for (int i = 0; i < 5; ++i) {
			philosophers[i] = new Philosopher(chopsticks[i], chopsticks[(i + 1) % 5]);
			philosophers[i].start();
		}
		System.out.println("启动完毕...");
		for (int i = 0; i < 5; ++i)
			philosophers[i].join();
		System.out.println("===end===");
	}
}
