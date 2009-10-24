package org.sodeja.rel;

public class AttributeMapping {
	public final Attribute source;
	public final Attribute target;
	
	public AttributeMapping(Attribute source, Attribute target) {
		if(! source.type.equals(target.type)) {
			throw new RuntimeException("Invalid attribute mapping");
		}
		
		this.source = source;
		this.target = target;
	}
}
