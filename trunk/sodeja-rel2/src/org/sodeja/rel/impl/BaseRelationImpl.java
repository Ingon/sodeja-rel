package org.sodeja.rel.impl;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.sodeja.functional.Pair;
import org.sodeja.rel.Attribute;
import org.sodeja.rel.BaseRelation;
import org.sodeja.rel.TupleSet;
import org.sodeja.rel.Type;

class BaseRelationImpl implements BaseRelation {
	protected final DomainImpl domain;
	protected final TransactionManagerImpl txManager;
	protected final String name;
	
	protected final SortedSet<Attribute> attributes;
//	protected final Set<Attribute> pk;
//	protected final Set<ForeignKey> fks;
	
	public BaseRelationImpl(DomainImpl domain, String name) {
		this.domain = domain;
		this.txManager = domain.txManager;
		this.name = name;
		
		this.attributes = new TreeSet<Attribute>();
//		this.pk = new TreeSet<Attribute>();
//		this.fks = new HashSet<ForeignKey>();
	}

	@Override
	public void addAttribute(String name, Type type) {
		attributes.add(new Attribute(name, type));
	}

	@Override
	public void removeAttribute(String name) {
		for(Iterator<Attribute> ite = attributes.iterator(); ite.hasNext(); ) {
			Attribute att = ite.next();
			if(att.name.equals(name)) {
				ite.remove();
			}
		}
	}

	@Override
	public void setPrimaryKey(Set<String> pkAttributes) {
	}

	@Override
	public void addForeignKey(BaseRelation other, Set<Pair<String, String>> fkToPkAttributes) {
	}

	@Override
	public void insert() {
	}

	@Override
	public void update() {
	}

	@Override
	public void delete() {
	}

	@Override
	public TupleSet select() {
		return null;
	}
}
