package org.sodeja.rel.relations;

import org.sodeja.rel.Relation;

public class ProjectRelation extends DerivedRelation {
	protected final String[] attributes;
	
	public ProjectRelation(String name, Relation relation, String[] attributes) {
		super(name, relation);
		this.attributes = attributes;
	}
}
