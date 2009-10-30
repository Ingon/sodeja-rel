package org.sodeja.rel.impl;

import org.sodeja.rel.BaseRelation;
import org.sodeja.rel.TupleSet;

class BaseRelationImpl implements BaseRelation {
	protected final String name;
	
	public BaseRelationImpl(String name) {
		this.name = name;
	}

	@Override
	public TupleSet select() {
		return null;
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
}
