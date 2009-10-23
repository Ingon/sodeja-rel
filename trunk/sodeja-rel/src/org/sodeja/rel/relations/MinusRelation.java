package org.sodeja.rel.relations;

import java.util.HashSet;
import java.util.Set;

import org.sodeja.rel.Entity;
import org.sodeja.rel.Relation;

public class MinusRelation extends BinaryRelation {
	public MinusRelation(String name, Relation left, Relation right) {
		super(name, left, right);
	}

	@Override
	public Set<Entity> select() {
		Set<Entity> leftEntities = left.select();
		Set<Entity> rightEntities = right.select();
		
		Set<Entity> result = new HashSet<Entity>(leftEntities);
		result.removeAll(rightEntities);
		return result;
	}
}
