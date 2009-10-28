package org.sodeja.rel;

public class Attribute implements Comparable<Attribute> {
	public final String name;
	public final Type type;
	
	public Attribute(String name, Type type) {
		this.name = name.intern();
		this.type = type;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + name.hashCode();
		result = prime * result + type.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj == this) {
			return true;
		}
		
		if(! (obj instanceof Attribute)) {
			return false;
		}
		
		Attribute o = (Attribute) obj;
		return name == o.name && type == o.type;
	}

	@Override
	public int compareTo(Attribute o) {
		return name.compareToIgnoreCase(o.name);
	}

	@Override
	public String toString() {
		return name;
	}
	
	public Attribute rename(String newName) {
		return new Attribute(newName, type);
	}
}
