package org.sodeja.rel;

public interface TransactionManager {
	public void begin(boolean readOnly);
	public void commit();
	public void rollback();
}
