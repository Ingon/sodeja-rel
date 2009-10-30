package org.sodeja.rel;

import java.util.Set;

public interface Tuple {
	public Set<String> getAttributeNames();
	public Set<Attribute> getAttributes();
	
	public Object get(String name);
	public Type getType(String name);
}
