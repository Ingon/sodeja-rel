package org.sodeja.rel;

import java.util.Set;

public class Aggregates {
	public static Aggregate count(final String exportName) {
		return new Aggregate() {
			@Override
			public Entity aggregate(Entity base, Set<Entity> entities) {
				AttributeValue nval = new AttributeValue(new Attribute(exportName, Types.INT), entities.size());
				return base.extend(nval);
			}
		};
	}
}
