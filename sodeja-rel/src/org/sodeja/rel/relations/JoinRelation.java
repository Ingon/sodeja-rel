package org.sodeja.rel.relations;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.sodeja.lang.ObjectUtils;
import org.sodeja.rel.AttributeValue;
import org.sodeja.rel.Entity;
import org.sodeja.rel.Relation;

public class JoinRelation extends BinaryRelation {
	public JoinRelation(String name, Relation left, Relation right) {
		super(name, left, right);
	}

	@Override
	public Set<Entity> select() {
		Set<Entity> leftResult = left.select();
		Set<Entity> rightResult = right.select();
		
		Set<Entity> result = new HashSet<Entity>();
		for(Entity left : leftResult) {
			for(Entity right : rightResult) {
				if(following(left, right)) {
					result.add(combine(left, right));
				}
			}
		}
		
		return result;
	}

	private boolean following(Entity left, Entity right) {
		for(AttributeValue lval : left.getValues()) {
			AttributeValue rval = right.getAttributeValue(lval.attribute.name);
			if(rval != null && !ObjectUtils.equalsIfNull(lval.value, rval.value)) {
				return false;
			}
		}
		return true;
	}

	private Entity combine(Entity left, Entity right) {
		SortedSet<AttributeValue> resultSet = new TreeSet<AttributeValue>(left.getValues());
		for(AttributeValue rval : right.getValues()) {
			if(left.getAttributeValue(rval.attribute.name) == null) {
				resultSet.add(rval);
			}
		}
		return new Entity(resultSet);
	}

}
