package org.sodeja.rel;

public interface TransactionManager {
	public void begin();
	public void commit();
	public void rollback();
}