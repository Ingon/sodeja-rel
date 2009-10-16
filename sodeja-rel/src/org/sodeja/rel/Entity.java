package org.sodeja.rel;

import java.util.Collections;
import java.util.Set;

import org.sodeja.collections.SetUtils;
import org.sodeja.functional.Function1;

public class Entity {
	protected final Set<AttributeValue> values;

	public Entity(Set<AttributeValue> values) {
		this.values = Collections.unmodifiableSet(values);
	}

	public Object getValue(String attribute) {
		return getAttributeValue(attribute).value;
	}
	
	public Set<AttributeValue> getValues() {
		return values;
	}

	public AttributeValue getAttributeValue(String attribute) {
		for(AttributeValue value : values) {
			if(value.attribute.name.equals(attribute)) {
				return value;
			}
		}
		throw new RuntimeException();
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
		return "(" + SetUtils.map(values, new Function1<String, AttributeValue>() {
			@Override
			public String execute(AttributeValue p) {
				return p.attribute.name + "::" + p.value;
			}}) + ")";
	}
}
