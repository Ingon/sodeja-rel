package org.sodeja.rel.relations;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.sodeja.collections.SetUtils;
import org.sodeja.lang.ObjectUtils;
import org.sodeja.rel.Aggregate;
import org.sodeja.rel.AttributeMapping;
import org.sodeja.rel.AttributeValue;
import org.sodeja.rel.Entity;
import org.sodeja.rel.Relation;

public class SummarizeRelation extends BinaryRelation {
	protected final Set<AttributeMapping> mappings;
	protected final Set<Aggregate> aggregates;
	
	public SummarizeRelation(String name, Relation left, Relation right, Aggregate aggregate) { // TODO maybe many ?
		this(name, left, right, null, SetUtils.asSet(aggregate));
	}

	public SummarizeRelation(String name, Relation left, Relation right, Set<AttributeMapping> mappings, Set<Aggregate> aggregates) { // TODO maybe many ?
		super(name, left, right);
		this.mappings = mappings;
		this.aggregates = aggregates;
	}
	
	@Override
	public Set<Entity> select() {
		Set<Entity> all = left.select();
		Set<Entity> grouping = right.select();

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
			Entity tmp = g.getKey();
			for(Aggregate a : aggregates) {
				tmp = a.aggregate(tmp, g.getValue());
			}
			result.add(tmp);
		}
		return result;
	}

	private boolean conained(Entity g, Entity a) {
		if(mappings != null) {
			return containedByMappings(g, a);
		} else {
			return conatinedByNames(g, a);
		}
	}

	private boolean containedByMappings(Entity g, Entity a) {
		for(AttributeMapping m : mappings) {
			Object gval = g.getValue(m.source.name);
			Object aval = a.getValue(m.target.name);
			if(! ObjectUtils.equalsIfNull(gval, aval)) {
				return false;
			}
		}
		return true;
	}

	private boolean conatinedByNames(Entity g, Entity a) {
		for(AttributeValue val : g.getValues()) {
			Object aval = a.getValue(val.attribute.name);
			if(! ObjectUtils.equalsIfNull(val.value, aval)) {
				return false;
			}
		}
		return true;
	}
}
