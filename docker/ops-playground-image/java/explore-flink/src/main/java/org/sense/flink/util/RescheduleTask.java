package org.sense.flink.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class RescheduleTask extends Thread {

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	private boolean running = false;
	private long period;

	public RescheduleTask() {
		this.running = true;
		this.period = 1000L;
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean running) {
		this.running = running;
		System.out.println("Changed running to " + this.running);
	}

	public long getPeriod() {
		return period;
	}

	public void setPeriod(long period) {
		this.period = period;
		System.out.println("Changed period to " + this.period);
	}

	@Override
	public void run() {
		while (running) {
			System.out.println("running... " + sdf.format(new Date(System.currentTimeMillis())));
			try {
				Thread.sleep(this.period);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws InterruptedException {
		RescheduleTask rescheduleTask = new RescheduleTask();
		rescheduleTask.start();
		Thread.sleep(10000);
		rescheduleTask.setPeriod(5000L);
		Thread.sleep(10000);
		rescheduleTask.setPeriod(1000L);
		Thread.sleep(10000);
		rescheduleTask.setPeriod(5000L);
		Thread.sleep(10000);
		rescheduleTask.setRunning(false);
	}
}
