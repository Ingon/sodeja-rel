package org.sodeja.rel;

public interface Type {
	public boolean accepts(Object o);
	public Object canonize(Object o);
}
