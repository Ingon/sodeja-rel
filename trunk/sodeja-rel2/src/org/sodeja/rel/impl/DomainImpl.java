package org.sodeja.rel.impl;

import java.util.HashMap;
import java.util.Map;

import org.sodeja.rel.BaseRelation;
import org.sodeja.rel.DerivedRelation;
import org.sodeja.rel.Domain;
import org.sodeja.rel.Relation;
import org.sodeja.rel.TransactionManager;

public class DomainImpl implements Domain {
	private final TransactionManager txManager;
	private final Map<String, BaseRelation> baseRelations;
	private final Map<String, DerivedRelation> derivedRelations;

	public DomainImpl() {
		txManager = new TransactionManagerImpl();
		baseRelations = new HashMap<String, BaseRelation>();
		derivedRelations = new HashMap<String, DerivedRelation>();
	}
	
	@Override
	public TransactionManager getTransactionManager() {
		return txManager;
	}

	@Override
	public BaseRelation newBaseRelation(String name) {
		BaseRelationImpl baseRelation = new BaseRelationImpl(name);
		baseRelations.put(name, baseRelation);
		return baseRelation;
	}

	@Override
	public void registerDerivedRelation(String name, DerivedRelation rel) {
		derivedRelations.put(name, rel);
	}

	@Override
	public Relation resolve(String name) {
		Relation rel = resolveDerived(name);
		if(rel != null) {
			return rel;
		}
		return resolveBase(name);
	}

	@Override
	public BaseRelation resolveBase(String name) {
		return baseRelations.get(name);
	}

	@Override
	public DerivedRelation resolveDerived(String name) {
		return derivedRelations.get(name);
	}
}
