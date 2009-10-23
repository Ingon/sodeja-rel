package org.sodeja.rel.relations;

import org.sodeja.rel.Relation;

public abstract class BinaryRelation extends UnaryRelation {

	protected final Relation left;
	
	public BinaryRelation(String name, Relation left, Relation right) {
		super(name, right);
		this.left = left;
	}
}
