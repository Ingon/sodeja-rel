package org.sodeja.rel;

public class AttributeValue implements Comparable<AttributeValue> {
	protected final Attribute attribute;
	protected final Object value;
	
	public AttributeValue(Attribute attribute, Object value) {
		this.attribute = attribute;
		this.value = value;
	}

	@Override
	public int compareTo(AttributeValue o) {
		return attribute.compareTo(o.attribute);
	}
}
