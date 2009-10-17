package org.sodeja.rel;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.sodeja.collections.CollectionUtils;
import org.sodeja.collections.SetUtils;
import org.sodeja.functional.Function1;
import org.sodeja.functional.Pair;
import org.sodeja.functional.Predicate1;
import org.sodeja.lang.ObjectUtils;
import org.sodeja.rel.relations.ProjectRelation;

public class BaseRelation implements Relation {
	protected final Domain domain;
	protected final String name;
	protected final Set<Attribute> attributes;
	protected final UUIDGenerator idGen = new UUIDGenerator();
	
	protected Set<Attribute> pk = new TreeSet<Attribute>();
	protected Map<BaseRelation, Set<Attribute>> fks = new HashMap<BaseRelation, Set<Attribute>>();
	protected Set<BaseRelation> references = new HashSet<BaseRelation>();

	protected BaseRelation(Domain domain, String name, Attribute... attributes) {
		this.domain = domain;
		this.name = name;
		this.attributes = new TreeSet<Attribute>(Arrays.asList(attributes));
		
		setEntities(Collections.<BaseEntity>emptySet(), Collections.<UUID>emptySet());
	}
	
	public BaseRelation primaryKey(String... attributeNames) {
		pk = resolveAttributes(attributeNames);
		return this;
	}
	
	public BaseRelation foreignKey(BaseRelation target, String... attributeNames) {
		Set<Attribute> candidateFk = resolveAttributes(attributeNames);
		target.reference(this, candidateFk);
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

	private void reference(BaseRelation source, Set<Attribute> candidateFk) {
		if(! this.pk.equals(candidateFk)) {
			throw new ConstraintViolationException("Foreign key set is not refering to primary key");
		}
		references.add(source);
	}
	
	public void insert(Set<Pair<String, Object>> attributeValues) {
		BaseEntity e = new BaseEntity(idGen.next(), extractValues(attributeValues));
		
		Set<BaseEntity> newValues = new HashSet<BaseEntity>(getEntities());
		newValues.add(e);
		setEntities(newValues, Collections.<UUID>emptySet());
	}

	public void update(Condition cond, Set<Pair<String, Object>> attributeValues) {
		Set<BaseEntity> entities = new HashSet<BaseEntity>();
		Set<UUID> delta = new HashSet<UUID>();
		for(BaseEntity e : getEntities()) {
			if(cond.satisfied(e)) {
				e = new BaseEntity(e.id, merge(e, attributeValues));
				delta.add(e.id);
			}
			entities.add(e);
		}
		setEntities(entities, delta);
	}
	
	private Set<AttributeValue> merge(BaseEntity e, Set<Pair<String, Object>> attributeValues) {
		Set<AttributeValue> newValues = new HashSet<AttributeValue>();
		newValues.addAll(e.getValues());
		
		for(Pair<String, Object> value : attributeValues) {
			AttributeValue oldValue = e.getAttributeValue(value.first);
			if(oldValue == null) {
				throw new RuntimeException("Unknown attribute " + value.first);
			}
			if(! oldValue.attribute.type.accepts(value.second)) {
				throw new RuntimeException("Wrong type for attribute " + value.first);
			}
			AttributeValue newValue = new AttributeValue(oldValue.attribute, value.second);
			newValues.remove(oldValue);
			newValues.add(newValue);
		}
		
		return newValues;
	}

	public void delete(Set<Pair<String, Object>> attributeValues) {
		final Set<AttributeValue> values = extractValues(attributeValues);
		delete(new Condition() {
			@Override
			public boolean satisfied(Entity e) {
				for(AttributeValue v : values) {
					if(! ObjectUtils.equalsIfNull(v.value, e.getAttributeValue(v.attribute).value)) {
						return false;
					}
				}
				return true;
			}
		});
	}
	
	public void delete(Condition cond) {
		Set<BaseEntity> entities = new HashSet<BaseEntity>();
		Set<UUID> delta = new HashSet<UUID>();
		for(BaseEntity e : getEntities()) {
			if(cond.satisfied(e)) {
				checkDeletion(e);
				delta.add(e.id);
			} else {
				entities.add(e);
			}
		}
		setEntities(entities, delta);
	}
	
	private void checkDeletion(Entity e) {
		for(BaseRelation rel : references) {
			Entity pkEntity = extract(e, pk);
			if(rel.refer(pkEntity)) {
				throw new ConstraintViolationException("Foreign key from " + rel.name + " relation violated");
			}
		}
	}

	private boolean refer(Entity pkEntity) {
		return selectByKey(pkEntity) != null;
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
		return Collections.<Entity>unmodifiableSet(getEntities());
	}
	
	private Entity extract(Entity e, Set<Attribute> attributes) {
		Set<AttributeValue> pkValues = new TreeSet<AttributeValue>();
		for(Attribute att : attributes) {
			pkValues.add(e.getAttributeValue(att.name));
		}
		return new Entity(pkValues);
	}

	protected Entity selectByKey(Entity pk) {
		OUTER: for(Entity e : getEntities()) {
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

	private Set<BaseEntity> getEntities() {
		return domain.getTransactionManager().get(this);
	}
	
	private void setEntities(Set<BaseEntity> entities, Set<UUID> delta) {
		domain.getTransactionManager().set(this, Collections.unmodifiableSet(entities), delta);
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

	// TODO checks are uneffective currently
	public void integrityChecks() {
		checkPrimaryKey();
		checkForeignKeys();
	}

	private void checkPrimaryKey() {
		ProjectRelation rel = new ProjectRelation(null, this, SetUtils.map(pk, new Function1<String, Attribute>() {
			@Override
			public String execute(Attribute p) {
				return p.name;
			}}));
		if(getEntities().size() != rel.select().size()) {
			throw new ConstraintViolationException("Primary key constraint violated");
		}
	}

	private void checkForeignKeys() {
		for(Entity e : getEntities()) {
			for(Map.Entry<BaseRelation, Set<Attribute>> rel : fks.entrySet()) {
				Entity fkEntity = extract(e, rel.getValue());
				if(rel.getKey().selectByKey(fkEntity) == null) {
					throw new ConstraintViolationException("Foreign key to " + rel.getKey().name + " violated");
				}
			}
		}
	}

}
