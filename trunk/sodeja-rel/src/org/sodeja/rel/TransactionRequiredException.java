package org.sodeja.rel;

public class TransactionRequiredException extends RuntimeException {
	private static final long serialVersionUID = -4420531638400547315L;

	public TransactionRequiredException(String message) {
		super(message);
	}
}
