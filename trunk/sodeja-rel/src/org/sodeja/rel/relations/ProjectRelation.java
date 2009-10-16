package org.sodeja.rel.relations;

import java.util.Set;
import java.util.TreeSet;

import org.sodeja.collections.SetUtils;
import org.sodeja.functional.Function1;
import org.sodeja.rel.AttributeValue;
import org.sodeja.rel.Entity;
import org.sodeja.rel.Relation;

public class ProjectRelation extends DerivedRelation {
	protected final String[] attributes;
	
	public ProjectRelation(String name, Relation relation, String[] attributes) {
		super(name, relation);
		this.attributes = attributes;
	}

	@Override
	public Set<Entity> select() {
		Set<Entity> entities = relation.select();
		return SetUtils.map(entities, new Function1<Entity, Entity>() {
			@Override
			public Entity execute(Entity p) {
				Set<AttributeValue> values = new TreeSet<AttributeValue>();
				for(String attribute : attributes) {
					values.add(p.getAttributeValue(attribute));
				}
				return new Entity(values);
			}});
	}
}
