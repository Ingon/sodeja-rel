package org.sodeja.rel;

public class RollbackException extends RuntimeException {
	private static final long serialVersionUID = 414557469536562108L;

	public RollbackException(String message) {
		super(message);
	}
}
