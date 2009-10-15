package org.sodeja.rel.relations;

import java.util.Set;
import java.util.TreeSet;

import org.sodeja.collections.SetUtils;
import org.sodeja.functional.Function1;
import org.sodeja.rel.AttributeValue;
import org.sodeja.rel.Entity;
import org.sodeja.rel.Relation;

public class ProjectAwayRelation extends DerivedRelation {
	protected final String[] attributes;
	
	public ProjectAwayRelation(String name, Relation relation, String[] attributes) {
		super(name, relation);
		this.attributes = attributes;
	}

	@Override
	public Set<Entity> select() {
		Set<Entity> entities = relation.select();
		return SetUtils.map(entities, new Function1<Entity, Entity>() {
			@Override
			public Entity execute(Entity p) {
				Set<AttributeValue> vals = p.getValues();
				Set<AttributeValue> filtered = new TreeSet<AttributeValue>();
				for(AttributeValue val : vals) {
					if(! shouldRemove(val)) {
						filtered.add(val);
					}
				}
				return new Entity(filtered);
			}});
	}

	protected boolean shouldRemove(AttributeValue val) {
		for(String att : attributes) {
			if(val.attribute.name.equals(att)) {
				return true;
			}
		}
		return false;
	}
}
