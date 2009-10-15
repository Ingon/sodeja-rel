package org.sodeja.rel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.sodeja.collections.ArrayUtils;
import org.sodeja.collections.CollectionUtils;
import org.sodeja.functional.Function1;
import org.sodeja.functional.Pair;
import org.sodeja.functional.Predicate1;

public class BaseRelation implements Relation {
	protected final String name;
	protected final Set<Attribute> attributes;
	
	protected Attribute[] pk = new Attribute[0];
	protected Map<Relation, Attribute[]> fks = new HashMap<Relation, Attribute[]>();

	protected Set<Entity> entities = new HashSet<Entity>();
	
	protected BaseRelation(String name, Attribute... attributes) {
		this.name = name;
		this.attributes = new TreeSet<Attribute>(Arrays.asList(attributes));
	}
	
	public BaseRelation primaryKey(String... attributeNames) {
		pk = resolveAttributes(attributeNames);
		return this;
	}
	
	public BaseRelation foreignKey(Relation target, String... attributeNames) {
		fks.put(target, resolveAttributes(attributeNames));
		return this;
	}

	private Attribute[] resolveAttributes(String... attributeNames) {
		return ArrayUtils.map(attributeNames, new Attribute[attributeNames.length], attributeFinder);
	}

	public void insert(Set<Pair<String, Object>> attributeValues) {
		entities.add(new Entity(extractValues(attributeValues)));
	}

	private Set<AttributeValue> extractValues(Set<Pair<String, Object>> attributeValues) {
		return (Set<AttributeValue>) 
			CollectionUtils.map(attributeValues, new TreeSet<AttributeValue>(), new Function1<AttributeValue, Pair<String, Object>>() {
			@Override
			public AttributeValue execute(Pair<String, Object> p) {
				return new AttributeValue(attributeFinder.execute(p.first), p.second);
			}});
	}

	@Override
	public Set<Entity> select() {
		return entities;
	}
	
	private Function1<Attribute, String> attributeFinder = new Function1<Attribute, String>() {
		@Override
		public Attribute execute(final String att) {
			return CollectionUtils.find(attributes, new Predicate1<Attribute>() {
				@Override
				public Boolean execute(Attribute p) {
					return p.name.equals(att);
				}});
		}};
}
