package org.sodeja.rel;

import java.util.Set;

public abstract class Aggregate {
	public abstract Entity aggregate(Entity base, Set<Entity> entities);
}
