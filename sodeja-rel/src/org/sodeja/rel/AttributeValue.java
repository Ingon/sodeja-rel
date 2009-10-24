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
		int temp = attribute.compareTo(o.attribute);
		if(temp == 0 && value instanceof Comparable<?>) {
			temp = ((Comparable) value).compareTo(o.value);
		}
		return temp;
	}

	@Override
	public String toString() {
		return attribute + "::" + value;
	}

	public AttributeValue rename(String newName) {
		return new AttributeValue(attribute.rename(newName), value);
	}
}
