package org.sodeja.rel;

class Alias implements Type {
	protected final Type delegate;

	protected Alias(Type delegate) {
		this.delegate = delegate;
	}
}
