package org.sodeja.rel;

public class AttributeValue implements Comparable<AttributeValue> {
	public final Attribute attribute;
	public final Object value;
	
	public AttributeValue(Attribute attribute, Object value) {
		this.attribute = attribute;
		this.value = value;
	}

	@Override
	public int compareTo(AttributeValue o) {
		return attribute.compareTo(o.attribute);
	}
}
