package org.sodeja.rel;

import java.util.Set;

class BaseEntity extends Entity {
	protected final long id;
	
	public BaseEntity(long id, Set<AttributeValue> values) {
		super(values);
		this.id = id;
	}

	@Override
	public boolean equals(Object obj) {
		if(! (obj instanceof BaseEntity)) {
			return false;
		}
		return this.id == ((BaseEntity) obj).id;
	}
}
