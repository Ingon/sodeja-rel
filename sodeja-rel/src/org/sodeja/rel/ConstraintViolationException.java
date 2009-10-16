package org.sodeja.rel;

public class ConstraintViolationException extends RuntimeException {
	private static final long serialVersionUID = 2568467287441724161L;

	public ConstraintViolationException(String message) {
		super(message);
	}
}
