package org.sodeja.rel.relations;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.sodeja.lang.ObjectUtils;
import org.sodeja.rel.Aggregate;
import org.sodeja.rel.AttributeValue;
import org.sodeja.rel.Entity;
import org.sodeja.rel.Relation;

public class SummarizeRelation extends DerivedRelation {
	protected final Relation other;
	protected final Aggregate aggregate;
	
	public SummarizeRelation(String name, Relation relation, Relation other, Aggregate aggregate) { // TODO maybe many ?
		super(name, relation);
		this.other = other;
		this.aggregate = aggregate;
	}

	@Override
	public Set<Entity> select() {
		Set<Entity> all = relation.select();
		Set<Entity> grouping = other.select();

		Map<Entity, Set<Entity>> groups = new HashMap<Entity, Set<Entity>>();
		for(Entity g : grouping) {
			Set<Entity> gval = new HashSet<Entity>();
			for(Entity a : all) {
				if(conained(g, a)) {
					gval.add(a);
				}
			}
			groups.put(g, gval);
		}
		
		Set<Entity> result = new HashSet<Entity>();
		for(Map.Entry<Entity, Set<Entity>> g : groups.entrySet()) {
			result.add(aggregate.aggregate(g.getValue()));
		}
		return result;
	}

	private boolean conained(Entity g, Entity a) {
		for(AttributeValue val : g.getValues()) {
			Object aval = a.getValue(val.attribute.name);
			if(! ObjectUtils.equalsIfNull(val.value, aval)) {
				return false;
			}
		}
		return true;
	}
}
