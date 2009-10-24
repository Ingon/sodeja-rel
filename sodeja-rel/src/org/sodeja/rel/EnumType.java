package org.sodeja.rel;

class EnumType implements Type {
	protected final Class<?> internal;

	protected EnumType(Class<?> internal) {
		this.internal = internal;
	}

	@Override
	public boolean accepts(Object o) {
		return internal == o.getClass();
	}

	@Override
	public Object canonize(Object o) {
		return o;
	}
}
