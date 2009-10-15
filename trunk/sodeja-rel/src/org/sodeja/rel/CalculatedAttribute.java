package org.sodeja.rel;

public abstract class CalculatedAttribute extends Attribute {
	public CalculatedAttribute(String name, Type type) {
		super(name, type);
	}
	
	public abstract Object calculate(Entity entity);
}
