package org.sodeja.rel.relations;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.sodeja.lang.ObjectUtils;
import org.sodeja.rel.AttributeMapping;
import org.sodeja.rel.AttributeValue;
import org.sodeja.rel.BaseRelation;
import org.sodeja.rel.Entity;
import org.sodeja.rel.Relation;

public class DetailMasterJoin extends DerivedRelation {
	protected final Relation other;
	protected final Set<AttributeMapping> mappings;
	
	public DetailMasterJoin(String name, BaseRelation relation, BaseRelation other) {
		this(name, relation, other, relation.getFkMapping(other));
	}

	public DetailMasterJoin(String name, Relation relation, Relation other, Set<AttributeMapping> mappings) {
		super(name, relation);
		
		this.other = other;
		this.mappings = mappings;
	}

	@Override
	public Set<Entity> select() {
		Set<Entity> leftResult = relation.select();
		Set<Entity> rightResult = other.select();
		
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
		for(AttributeMapping att : mappings) {
			Object lval = left.getValue(att.source.name);
			Object rval = right.getValue(att.target.name);
			if(!ObjectUtils.equalsIfNull(lval, rval)) {
				return false;
			}
		}
		return true;
	}

	private Entity combine(Entity left, Entity right) {
		Set<AttributeValue> resultSet = new TreeSet<AttributeValue>(left.getValues());
		for(AttributeValue rval : right.getValues()) {
			if(left.getAttributeValue(rval.attribute.name) == null) {
				resultSet.add(rval);
			}
		}
		return new Entity(resultSet);
	}
}
