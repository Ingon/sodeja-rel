package org.sodeja.rel;

import java.util.Set;

public abstract class Aggregate {
	public abstract Entity aggregate(Set<Entity> entities);
}
