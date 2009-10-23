package org.sodeja.rel.relations;

import org.sodeja.rel.Relation;

public abstract class UnaryRelation implements Relation {
	private final String name;
	protected final Relation right;
	
	public UnaryRelation(String name, Relation right) {
		this.name = name;
		this.right = right;
	}

	@Override
	public String getName() {
		return name;
	}
}
