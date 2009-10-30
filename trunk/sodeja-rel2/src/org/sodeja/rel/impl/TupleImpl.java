package org.sodeja.rel.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;

import org.sodeja.collections.SetUtils;
import org.sodeja.functional.Function1;
import org.sodeja.rel.Attribute;
import org.sodeja.rel.Tuple;
import org.sodeja.rel.Type;

class TupleImpl implements Tuple {
	protected final SortedSet<AttributeValue> values;
	
	private final AttributeValue[] valuesCache;
	private final int hashCodeCache;

	public TupleImpl(SortedSet<AttributeValue> values) {
		this.values = Collections.unmodifiableSortedSet(values);
		this.valuesCache = this.values.toArray(new AttributeValue[this.values.size()]);
		this.hashCodeCache = Arrays.hashCode(this.valuesCache);
	}
	
	@Override
	public Object get(String name) {
		return getAttributeValue(name).value;
	}

	@Override
	public Type getType(String name) {
		return getAttributeValue(name).attribute.type;
	}

	@Override
	public Set<String> getAttributeNames() {
		return SetUtils.maps(values, new Function1<String, AttributeValue>() {
			@Override
			public String execute(AttributeValue p) {
				return p.attribute.name;
			}});
	}

	@Override
	public Set<Attribute> getAttributes() {
		return SetUtils.maps(values, new Function1<Attribute, AttributeValue>() {
			@Override
			public Attribute execute(AttributeValue p) {
				return p.attribute;
			}});
	}

	private AttributeValue getAttributeValue(String name) {
		for(AttributeValue v : values) {
			if(v.attribute.name.equals(name)) {
				return v;
			}
		}
		throw new RuntimeException("Attribute " + name + " not present");
	}
	
	@Override
	public boolean equals(Object obj) {
		if(this == obj) {
			return true;
		}
		if(! (obj instanceof TupleImpl)) {
			return false;
		}
		return Arrays.equals(valuesCache, ((TupleImpl) obj).valuesCache);
	}

	@Override
	public int hashCode() {
		return hashCodeCache;
	}
}
