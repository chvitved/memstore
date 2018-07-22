package com.datagenerator;

public class ProfileRunner {
	
	public static void main(String[] args) throws InterruptedException {
		PersonGenerator.main(args);
		System.out.println("done running");
		Thread.sleep(60000);
	}

}
