package org.sodeja.rel;

import java.security.SecureRandom;
import java.util.UUID;

public class UUIDGenerator {
	private final SecureRandom rand = new SecureRandom();
	
	public UUIDGenerator() {
	}
	
	public UUID next() {
		return new UUID(rand.nextLong(), rand.nextLong());
	}
}
