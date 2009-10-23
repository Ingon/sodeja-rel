package org.sodeja.rel;

import java.util.Set;

public interface Relation {
	public String getName();
	public Set<Entity> select();
}
