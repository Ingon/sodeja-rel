package org.sodeja.rel.relations;

import org.sodeja.rel.Relation;

public class MinusRelation extends DerivedRelation {
	protected final Relation other;

	public MinusRelation(String name, Relation relation, Relation other) {
		super(name, relation);
		this.other = other;
	}
}
