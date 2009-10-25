package org.sodeja.rel;

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.sodeja.collections.SetUtils;

public class Entity {
	protected final SortedSet<AttributeValue> values;

	public Entity(SortedSet<AttributeValue> values) {
		this.values = Collections.unmodifiableSortedSet(values);
	}

	public Object getValue(String attribute) {
		AttributeValue val = getAttributeValue(attribute);
		return val != null ? val.value : null;
	}
	
	public Set<AttributeValue> getValues() {
		return values;
	}

	public AttributeValue getAttributeValue(Attribute attribute) {
		for(AttributeValue value : values) {
			if(value.attribute.equals(attribute)) {
				return value;
			}
		}
		return null;
	}
	
	public AttributeValue getAttributeValue(String attribute) {
		for(AttributeValue value : values) {
			if(value.attribute.name.equals(attribute)) {
				return value;
			}
		}
		return null;
	}
	
	@Override
	public int hashCode() {
		return values.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if(! (obj instanceof Entity)) {
			return false;
		}
		return values.equals(((Entity) obj).values);
	}

	@Override
	public String toString() {
		return values.toString();
	}
	
	public Entity extend(AttributeValue... vals) {
		SortedSet<AttributeValue> nvals = new TreeSet<AttributeValue>(values);
		nvals.addAll(SetUtils.asSet(vals));
		return new Entity(nvals);
	}
}
