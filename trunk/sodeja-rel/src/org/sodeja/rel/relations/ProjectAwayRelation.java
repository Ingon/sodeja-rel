package org.sodeja.rel.relations;

import org.sodeja.rel.Relation;

public class ProjectAwayRelation extends DerivedRelation {
	protected final String[] attributes;
	
	public ProjectAwayRelation(String name, Relation relation, String[] attributes) {
		super(name, relation);
		this.attributes = attributes;
	}
}
