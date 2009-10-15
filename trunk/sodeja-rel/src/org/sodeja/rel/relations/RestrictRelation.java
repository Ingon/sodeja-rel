package org.sodeja.rel.relations;

import org.sodeja.rel.Condition;
import org.sodeja.rel.Relation;

public class RestrictRelation extends DerivedRelation {
	protected final Condition[] conditions;
	
	public RestrictRelation(String name, Relation relation, Condition[] conditions) {
		super(name, relation);
		this.conditions = conditions;
	}
}
