package org.sodeja.rel.relations;

import java.util.HashSet;
import java.util.Set;

import org.sodeja.rel.Entity;
import org.sodeja.rel.Relation;

public class MinusRelation extends DerivedRelation {
	protected final Relation other;

	public MinusRelation(String name, Relation relation, Relation other) {
		super(name, relation);
		this.other = other;
	}

	@Override
	public Set<Entity> select() {
		Set<Entity> leftEntities = relation.select();
		Set<Entity> rightEntities = other.select();
		
		Set<Entity> result = new HashSet<Entity>(leftEntities);
		result.removeAll(rightEntities);
		return result;
	}
}
