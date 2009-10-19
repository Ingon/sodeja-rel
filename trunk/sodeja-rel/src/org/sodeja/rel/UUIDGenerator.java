package org.sodeja.rel;

import java.util.concurrent.atomic.AtomicInteger;

public class UUIDGenerator {
	private AtomicInteger value = new AtomicInteger();
	
	public UUIDGenerator() {
	}
	
	public UUID next() {
		return new UUID(value.getAndIncrement());
	}
}
