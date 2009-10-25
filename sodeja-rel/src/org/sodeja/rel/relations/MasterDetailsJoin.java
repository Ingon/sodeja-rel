package org.sodeja.rel.relations;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.sodeja.lang.ObjectUtils;
import org.sodeja.rel.Attribute;
import org.sodeja.rel.AttributeMapping;
import org.sodeja.rel.AttributeValue;
import org.sodeja.rel.BaseRelation;
import org.sodeja.rel.Entity;
import org.sodeja.rel.Relation;
import org.sodeja.rel.Types;

public class MasterDetailsJoin extends BinaryRelation {
	protected final Set<AttributeMapping> mappings;
	protected final String exportName;
	
	public MasterDetailsJoin(String name, BaseRelation master, BaseRelation details, String exportName) {
		this(name, master, details, details.getFk(master).invertedMapping(), exportName);
	}
	
	public MasterDetailsJoin(String name, Relation left, Relation right, Set<AttributeMapping> mappings, String exportName) {
		super(name, left, right);
		this.mappings = mappings;
		this.exportName = exportName;
	}

	@Override
	public Set<Entity> select() {
		Set<Entity> masterEntities = left.select();
		Set<Entity> detailsEntities = right.select();
		
		Map<Entity, Set<Entity>> masterDetails = new HashMap<Entity, Set<Entity>>();
		for(Entity master : masterEntities) {
			masterDetails.put(master, new HashSet<Entity>());
			for(Entity detail : detailsEntities) {
				if(connected(master, detail)) {
					masterDetails.get(master).add(detail);
				}
			}
		}
		
		Attribute a = new Attribute(exportName, Types.SET);
		Set<Entity> result = new HashSet<Entity>();
		for(Map.Entry<Entity, Set<Entity>> e : masterDetails.entrySet()) {
			result.add(e.getKey().extend(new AttributeValue(a, e.getValue())));
		}
		return result;
	}

	public MasterDetailsJoin copyOn(Relation newBase) {
		return new MasterDetailsJoin(getName(), newBase, right, mappings, exportName);
	}
	
	private boolean connected(Entity master, Entity detail) {
		for(AttributeMapping m : mappings) {
			Object mval = master.getValue(m.source.name);
			Object dval = detail.getValue(m.target.name);
			if(! ObjectUtils.equalsIfNull(mval, dval)) {
				return false;
			}
		}
		return true;
	}
}
