package org.sodeja.rel;

class Alias implements Type {
	protected final Type delegate;

	protected Alias(Type delegate) {
		this.delegate = delegate;
	}

	@Override
	public boolean accepts(Object o) {
		return delegate.accepts(o);
	}
}
