package org.sodeja.rel;

import org.sodeja.lang.ObjectUtils;

public class AttributeValue implements Comparable<AttributeValue> {
	public final Attribute attribute;
	public final Object value;
	
	public AttributeValue(Attribute attribute, Object value) {
		this.attribute = attribute;
		this.value = value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + attribute.hashCode();
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(! (obj instanceof AttributeValue)) {
			return false;
		}
		AttributeValue o = (AttributeValue) obj;
		return attribute.equals(o.attribute) && ObjectUtils.equalsIfNull(value, o.value);
	}

	@Override
	public int compareTo(AttributeValue o) {
		return attribute.compareTo(o.attribute);
	}

	@Override
	public String toString() {
		return attribute + "::" + value;
	}
}
