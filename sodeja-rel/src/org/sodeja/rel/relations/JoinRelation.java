package org.sodeja.rel.relations;

import org.sodeja.rel.Relation;

public class JoinRelation extends DerivedRelation {
	protected final Relation other;
	
	public JoinRelation(String name, Relation relation, Relation other) {
		super(name, relation);
		this.other = other;
	}
}
