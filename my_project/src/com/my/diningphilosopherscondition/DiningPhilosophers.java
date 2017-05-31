package com.my.diningphilosopherscondition;

import java.util.concurrent.locks.ReentrantLock;

public class DiningPhilosophers {
	static String im = "";
	String im2 = "i am im2.";

	class TestII {
		public void type() {
			System.out.println(im2);
		}
	};
	
	public void test(){
		TestII tt = new TestII();
		tt.type();
	}

	public static void main(String[] args) throws InterruptedException {
		final Philosopher[] philosophers = new Philosopher[5];
		final ReentrantLock table = new ReentrantLock();

		for (int i = 0; i < 5; ++i)
			philosophers[i] = new Philosopher(table);
		for (int i = 0; i < 5; ++i) {
			philosophers[i].setLeft(philosophers[(i + 4) % 5]);
			philosophers[i].setRight(philosophers[(i + 1) % 5]);
			philosophers[i].start();
		}

		class CountingThread extends Thread {
			@SuppressWarnings("static-access")
			public void run() {
				System.out.println(im);
				while (!interrupted()) {
					try {
						currentThread().sleep(100);
					} catch (InterruptedException e) {
					}
					for (int i = 0; i < 5; ++i) {
						System.out.print("\r" + i + "号哲学家: " + (philosophers[i].isEating() ? "正在进餐" : "思考中..."));
					}
					System.out.println("\r==================");
					System.out.flush();
				}
			}
		}
		CountingThread t3 = new CountingThread();
		t3.start();
		for (int i = 0; i < 5; ++i)
			philosophers[i].join();
		t3.interrupt();
	}

}
