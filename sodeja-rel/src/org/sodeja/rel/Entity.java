package org.sodeja.rel;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.sodeja.collections.SetUtils;

public class Entity {
	protected final SortedSet<AttributeValue> values;
	
	private final AttributeValue[] valuesCache;
	private final int hashCodeCache;

	public Entity(SortedSet<AttributeValue> values) {
		this.values = Collections.unmodifiableSortedSet(values);
		this.valuesCache = this.values.toArray(new AttributeValue[this.values.size()]);
		this.hashCodeCache = Arrays.hashCode(this.valuesCache);
	}

	public Object getValue(String attribute) {
		AttributeValue val = getAttributeValue(attribute);
		return val != null ? val.value : null;
	}
	
	public Set<AttributeValue> getValues() {
		return values;
	}

	public AttributeValue getAttributeValue(Attribute attribute) {
		return getAttributeValue(attribute.name);
	}
	
	public AttributeValue getAttributeValue(String attribute) {
		for(AttributeValue value : valuesCache) {
			if(value.attribute.name.equals(attribute)) {
				return value;
			}
		}
		return null;
	}
	
	@Override
	public int hashCode() {
		return hashCodeCache;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(this == obj) {
			return true;
		}
		if(! (obj instanceof Entity)) {
			return false;
		}
		return Arrays.equals(valuesCache, ((Entity) obj).valuesCache);
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
	
	public boolean onlyNulls() {
		for(AttributeValue v : values) {
			if(v.value != null) {
				return false;
			}
		}
		return true;
	}
}
