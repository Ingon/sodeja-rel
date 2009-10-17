package org.sodeja.rel;

import java.util.Set;

import org.sodeja.collections.SetUtils;
import org.sodeja.functional.Pair;
import org.sodeja.lang.ObjectUtils;

public class Conditions {
	public static Condition likeValues(Object... namedValues) {
		final Set<Pair<String, Object>> named = SetUtils.namedValuesToSet(namedValues);
		return new Condition() {
			@Override
			public boolean satisfied(Entity e) {
				for(Pair<String, Object> val : named) {
					AttributeValue eval = e.getAttributeValue(val.first);
					if(! eval.attribute.type.accepts(val.second)) {
						throw new RuntimeException("Invalid data type for " + val.first);
					}
					if(! ObjectUtils.equalsIfNull(val.second, eval.value)) {
						return false;
					}
				}
				return true;
			}
		};
	}
}
