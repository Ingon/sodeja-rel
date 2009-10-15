package org.sodeja.rel;

import java.util.Set;

import org.sodeja.collections.SetUtils;
import org.sodeja.functional.Function1;

public class Entity {
	protected final Set<AttributeValue> values;

	public Entity(Set<AttributeValue> values) {
		this.values = values;
	}

	public Object getValue(String attribute) {
		for(AttributeValue value : values) {
			if(value.attribute.name.equals(attribute)) {
				return value.value;
			}
		}
		throw new RuntimeException();
	}
	
	public Set<AttributeValue> getValues() {
		return values;
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
