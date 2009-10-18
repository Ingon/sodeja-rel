package org.sodeja.rel;

interface TransactionManager {

	public abstract void begin();

	public abstract void commit();

	public abstract void rollback();

}