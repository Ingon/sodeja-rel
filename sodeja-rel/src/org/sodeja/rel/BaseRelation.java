package org.sodeja.rel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.sodeja.collections.CollectionUtils;
import org.sodeja.functional.Function1;
import org.sodeja.functional.Pair;
import org.sodeja.functional.Predicate1;
import org.sodeja.lang.ObjectUtils;

public class BaseRelation implements Relation {
	protected final String name;
	protected final Set<Attribute> attributes;
	
	protected Set<Attribute> pk = new TreeSet<Attribute>();
	protected Map<BaseRelation, Set<Attribute>> fks = new HashMap<BaseRelation, Set<Attribute>>();

	protected Set<Entity> entities = new HashSet<Entity>();
	
	protected BaseRelation(String name, Attribute... attributes) {
		this.name = name;
		this.attributes = new TreeSet<Attribute>(Arrays.asList(attributes));
	}
	
	public BaseRelation primaryKey(String... attributeNames) {
		pk = resolveAttributes(attributeNames);
		return this;
	}
	
	public BaseRelation foreignKey(BaseRelation target, String... attributeNames) {
		Set<Attribute> candidateFk = resolveAttributes(attributeNames);
		if(! target.pk.equals(candidateFk)) {
			throw new ConstraintViolationException("Foreign key set is not refering to primary key");
		}
		fks.put(target, candidateFk);
		return this;
	}

	private Set<Attribute> resolveAttributes(String... attributeNames) {
		Set<Attribute> att = new TreeSet<Attribute>();
		for(String name : attributeNames) {
			att.add(attributeFinder.execute(name));
		}
		return att;
	}

	public void insert(Set<Pair<String, Object>> attributeValues) {
		Entity e = new Entity(extractValues(attributeValues));
		checkPrimaryKey(e);
		checkForeignKeys(e);
		entities.add(e);
	}

	private void checkPrimaryKey(Entity e) {
		Entity pkEntity = extract(e, pk);
		if(selectByPk(pkEntity) != null) {
			throw new ConstraintViolationException("Primary key constraint violated");
		}
	}

	private void checkForeignKeys(Entity e) {
		for(Map.Entry<BaseRelation, Set<Attribute>> rel : fks.entrySet()) {
			Entity fkEntity = extract(e, rel.getValue());
			if(rel.getKey().selectByPk(fkEntity) == null) {
				throw new ConstraintViolationException("Foreign key to " + rel.getKey().name + " violated");
			}
		}
	}

	private Set<AttributeValue> extractValues(Set<Pair<String, Object>> attributeValues) {
		return (Set<AttributeValue>) 
			CollectionUtils.map(attributeValues, new TreeSet<AttributeValue>(), new Function1<AttributeValue, Pair<String, Object>>() {
			@Override
			public AttributeValue execute(Pair<String, Object> p) {
				Attribute att = attributeFinder.execute(p.first);
				if(! att.type.accepts(p.second)) {
					throw new ConstraintViolationException("Wrong type for " + p.first);
				}
				return new AttributeValue(att, p.second);
			}});
	}

	@Override
	public Set<Entity> select() {
		return entities;
	}
	
	private Entity extract(Entity e, Set<Attribute> attributes) {
		Set<AttributeValue> pkValues = new TreeSet<AttributeValue>();
		for(Attribute att : attributes) {
			pkValues.add(e.getAttributeValue(att.name));
		}
		return new Entity(pkValues);
	}

	protected Entity selectByPk(Entity pk) {
		OUTER: for(Entity e : entities) {
			for(AttributeValue pkVal : pk.getValues()) {
				Object eval = e.getValue(pkVal.attribute.name);
				if(! ObjectUtils.equalsIfNull(pkVal.value, eval)) {
					continue OUTER;
				}
			}
			return e;
		}
		return null;
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
