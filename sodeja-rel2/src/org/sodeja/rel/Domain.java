package org.sodeja.rel;

public interface Domain {
	public TransactionManager getTransactionManager();
	public BaseRelation newBaseRelation(String name);
	public void registerDerivedRelation(String name, DerivedRelation rel);
	
	public Relation resolve(String name);
	public BaseRelation resolveBase(String name);
	public DerivedRelation resolveDerived(String name);
}
