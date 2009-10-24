package org.sodeja.rel.relations;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.sodeja.lang.ObjectUtils;
import org.sodeja.rel.AttributeMapping;
import org.sodeja.rel.AttributeValue;
import org.sodeja.rel.BaseRelation;
import org.sodeja.rel.Entity;
import org.sodeja.rel.Relation;

public class DetailMasterJoin extends BinaryRelation {
	protected final Set<AttributeMapping> mappings;
	
	public DetailMasterJoin(String name, BaseRelation left, BaseRelation right) {
		super(name, left, right);
		this.mappings = left.getFkMapping(right);
	}
	
	public DetailMasterJoin(String name, BaseRelation left, BaseRelation right, String... attributes) {
		super(name, left, right);
		this.mappings = left.getFkMapping(right, attributes);
		if(this.mappings == null) {
			throw new RuntimeException("Foreign key to " + right.getName() + " on these attributes not found");
		}
	}

	public DetailMasterJoin(String name, Relation left, Relation right, Set<AttributeMapping> mappings) {
		super(name, left, right);
		this.mappings = mappings;
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
		SortedSet<AttributeValue> resultSet = new TreeSet<AttributeValue>(left.getValues());
		for(AttributeValue rval : right.getValues()) {
			if(left.getAttributeValue(rval.attribute.name) == null) {
				resultSet.add(rval);
			}
		}
		return new Entity(resultSet);
	}
}
