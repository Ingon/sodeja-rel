package org.sodeja.rel;

class EnumType implements Type {
	protected final Class<?> internal;

	protected EnumType(Class<?> internal) {
		this.internal = internal;
	}
}
