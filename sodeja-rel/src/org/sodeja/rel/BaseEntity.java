package org.sodeja.rel;

import java.util.Set;
import java.util.UUID;

class BaseEntity extends Entity {
	protected final UUID id;
	
	public BaseEntity(UUID id, Set<AttributeValue> values) {
		super(values);
		this.id = id;
	}

}
