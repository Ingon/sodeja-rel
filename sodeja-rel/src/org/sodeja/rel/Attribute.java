package org.sodeja.rel;

public class Attribute implements Comparable<Attribute> {
	protected final String name;
	protected final Type type;
	
	public Attribute(String name, Type type) {
		this.name = name;
		this.type = type;
	}

	@Override
	public int compareTo(Attribute o) {
		return name.compareToIgnoreCase(o.name);
	}
}
