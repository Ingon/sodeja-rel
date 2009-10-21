package org.sodeja.rel;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.sodeja.collections.ArrayUtils;
import org.sodeja.collections.CollectionUtils;
import org.sodeja.collections.PersistentMap;
import org.sodeja.collections.PersistentSet;
import org.sodeja.collections.SetUtils;
import org.sodeja.functional.Function1;
import org.sodeja.functional.Pair;
import org.sodeja.functional.Predicate1;
import org.sodeja.lang.IDGenerator;
import org.sodeja.lang.ObjectUtils;
import org.sodeja.lang.Range;

public class BaseRelation implements Relation {
	protected final Domain domain;
	protected final String name;
	protected final Set<Attribute> attributes;
	protected final IDGenerator idGen = new IDGenerator();
	
	protected Set<Attribute> pk = new TreeSet<Attribute>();
	protected Map<BaseRelation, Set<AttributeMapping>> fks = new HashMap<BaseRelation, Set<AttributeMapping>>();

	protected BaseRelation(Domain domain, String name, Attribute... attributes) {
		this.domain = domain;
		this.name = name;
		this.attributes = new TreeSet<Attribute>(Arrays.asList(attributes));
		
		setInfo(new BaseRelationInfo());
	}
	
	public BaseRelation primaryKey(String... attributeNames) {
		pk = resolveAttributes(attributeNames);
		BaseRelationInfo info = getInfo();
		setInfo(info.copyDelta(info.entities, info.entityMap, new BaseRelationIndex(pk), info.fkIndexes));
		
		return this;
	}
	
	public BaseRelation foreignKeyDirect(BaseRelation target, String... attributeNames) {
		String[] attributeNamesCouples = ArrayUtils.dup(attributeNames);
		return foreignKey(target, attributeNamesCouples);
	}
	
	public BaseRelation foreignKey(BaseRelation target, String... attributeNamesCouples) {
		if(attributeNamesCouples.length % 2 != 0) {
			throw new RuntimeException("Expected foreign - external pk couples");
		}
		
		String[][] tuples = ArrayUtils.unmapTuples(attributeNamesCouples, 2);
		String[] thisAttributes = tuples[0];
		String[] foreignAttributes = tuples[1];
		
		Set<Attribute> fkIndexAttributes = new TreeSet<Attribute>();
		Set<Attribute> targetPkCandidate = new TreeSet<Attribute>();
		
		Set<AttributeMapping> mapping = new HashSet<AttributeMapping>();
		for(int i : Range.of(thisAttributes)) {
			Attribute thisAtt = attributeFinder.execute(thisAttributes[i]);
			fkIndexAttributes.add(thisAtt);
			
			Attribute foreignAtt = target.attributeFinder.execute(foreignAttributes[i]);
			targetPkCandidate.add(foreignAtt);
			
			mapping.add(new AttributeMapping(thisAtt, foreignAtt));
		}
		
		if(! target.pk.equals(targetPkCandidate)) {
			throw new ConstraintViolationException("Foreign key set is not refering to primary key");
		}
		fks.put(target, mapping);
		
		BaseRelationInfo info = getInfo();
		setInfo(info.copyDelta(info.entities, info.entityMap, info.pkIndex, info.fkIndexes.addIndex(new BaseRelationIndex(fkIndexAttributes))));
		
		return this;
	}

	private Set<Attribute> resolveAttributes(String... attributeNames) {
		Set<Attribute> atts = new TreeSet<Attribute>();
		for(String name : attributeNames) {
			Attribute att = attributeFinder.execute(name);
			if(att == null) {
				throw new RuntimeException("Unknown attribute name");
			}
			atts.add(att);
		}
		return atts;
	}

	public void insert(Set<Pair<String, Object>> attributeValues) {
		BaseEntity e = new BaseEntity(idGen.next(), extractValues(attributeValues));
		
		BaseRelationInfo currentInfo = getInfo();
		
		PersistentSet<BaseEntity> entities = currentInfo.entities.addValue(e);
		PersistentMap<Long, BaseEntity> entityMap = currentInfo.entityMap.putValue(e.id, e);
		
		BaseRelationIndex newPkIndex = currentInfo.pkIndex.insert(e);
		BaseRelationIndexes newFkIndexes = currentInfo.fkIndexes.insert(e);
		
		currentInfo.newSet.add(e.id);
		
		setInfo(currentInfo.copyDelta(entities, entityMap, newPkIndex, newFkIndexes));
	}

	public void update(Set<Pair<String, Object>> attributeValues, Set<Pair<String, Object>> newAttributeValues) {
		final Set<AttributeValue> values = extractValues(attributeValues);
		Set<AttributeValue> pkValues = (Set<AttributeValue>) CollectionUtils.filter(values, new TreeSet<AttributeValue>(), new Predicate1<AttributeValue>() {
			@Override
			public Boolean execute(AttributeValue p) {
				return pk.contains(p.attribute);
			}});
		if(pkValues.size() == pk.size()) {
			Entity pk = new Entity(pkValues);
			PersistentSet<BaseEntity> entities = getInfo().pkIndex.find(pk);
			if(pkValues.size() == values.size()) {
				updateAll(entities, newAttributeValues);
			} else {
				values.removeAll(pkValues);
				
				update(new Condition() {
					@Override
					public boolean satisfied(Entity e) {
						for(AttributeValue v : values) {
							if(! ObjectUtils.equalsIfNull(v.value, e.getAttributeValue(v.attribute).value)) {
								return false;
							}
						}
						return true;
					}
				}, newAttributeValues);
			}
		} else {
			update(new Condition() {
				@Override
				public boolean satisfied(Entity e) {
					for(AttributeValue v : values) {
						if(! ObjectUtils.equalsIfNull(v.value, e.getAttributeValue(v.attribute).value)) {
							return false;
						}
					}
					return true;
				}
			}, newAttributeValues);
		}
	}
	
	public void update(Condition cond, Set<Pair<String, Object>> attributeValues) {
		BaseRelationInfo currentInfo = getInfo();
		
		PersistentSet<BaseEntity> entities = currentInfo.entities;
		PersistentMap<Long, BaseEntity> entityMap = currentInfo.entityMap;
		
		BaseRelationIndex newPkIndex = currentInfo.pkIndex;
		BaseRelationIndexes newFkIndexes = currentInfo.fkIndexes;

		for(BaseEntity e : entities) {
			if(cond.satisfied(e)) {
				entities = entities.removeValue(e);
				
				newPkIndex = newPkIndex.delete(e);
				newFkIndexes = newFkIndexes.delete(e);
				
				e = new BaseEntity(e.id, merge(e, attributeValues));
				
				entities = entities.addValue(e);
				entityMap = entityMap.putValue(e.id, e);
				
				newPkIndex = newPkIndex.insert(e);
				newFkIndexes = newFkIndexes.insert(e);
				
				currentInfo.updateSet.add(e.id);
			}
		}
		
		setInfo(currentInfo.copyDelta(entities, entityMap, newPkIndex, newFkIndexes));
	}
	
	private void updateAll(PersistentSet<BaseEntity> targetEntities, Set<Pair<String, Object>> attributeValues) {
		BaseRelationInfo currentInfo = getInfo();
		
		PersistentSet<BaseEntity> entities = currentInfo.entities;
		PersistentMap<Long, BaseEntity> entityMap = currentInfo.entityMap;
		
		BaseRelationIndex newPkIndex = currentInfo.pkIndex;
		BaseRelationIndexes newFkIndexes = currentInfo.fkIndexes;

		for(BaseEntity e : targetEntities) {
			entities = entities.removeValue(e);
			
			newPkIndex = newPkIndex.delete(e);
			newFkIndexes = newFkIndexes.delete(e);
			
			e = new BaseEntity(e.id, merge(e, attributeValues));
			
			entities = entities.addValue(e);
			entityMap = entityMap.putValue(e.id, e);
			
			newPkIndex = newPkIndex.insert(e);
			newFkIndexes = newFkIndexes.insert(e);
			
			currentInfo.updateSet.add(e.id);
		}
		
		setInfo(currentInfo.copyDelta(entities, entityMap, newPkIndex, newFkIndexes));
	}

	private Set<AttributeValue> merge(BaseEntity e, Set<Pair<String, Object>> attributeValues) {
		Set<AttributeValue> newValues = new TreeSet<AttributeValue>();
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
		Set<AttributeValue> pkValues = (Set<AttributeValue>) CollectionUtils.filter(values, new TreeSet<AttributeValue>(), new Predicate1<AttributeValue>() {
			@Override
			public Boolean execute(AttributeValue p) {
				return pk.contains(p.attribute);
			}});
		if(pkValues.size() == pk.size()) {
			Entity pk = new Entity(pkValues);
			PersistentSet<BaseEntity> entities = getInfo().pkIndex.find(pk);
			if(pkValues.size() == values.size()) {
				deleteAll(entities);
			} else {
				values.removeAll(pkValues);
				
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
		} else {
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
	}

	public void delete(Condition cond) {
		BaseRelationInfo currentInfo = getInfo();
		
		PersistentSet<BaseEntity> entities = currentInfo.entities;
		PersistentMap<Long, BaseEntity> entityMap = currentInfo.entityMap;
		
		BaseRelationIndex newPkIndex = currentInfo.pkIndex;
		BaseRelationIndexes newFkIndexes = currentInfo.fkIndexes;

		for(BaseEntity e : entities) {
			if(cond.satisfied(e)) {
				entities = entities.removeValue(e);
				entityMap = entityMap.removeValue(e.id);
				
				newPkIndex = newPkIndex.delete(e);
				newFkIndexes = newFkIndexes.delete(e);
				
				currentInfo.deleteSet.add(e.id);
			}
		}
		
		setInfo(currentInfo.copyDelta(entities, entityMap, newPkIndex, newFkIndexes));
	}

	private void deleteAll(Set<BaseEntity> targetEntities) {
		BaseRelationInfo currentInfo = getInfo();
		
		PersistentSet<BaseEntity> entities = currentInfo.entities;
		PersistentMap<Long, BaseEntity> entityMap = currentInfo.entityMap;
		
		BaseRelationIndex newPkIndex = currentInfo.pkIndex;
		BaseRelationIndexes newFkIndexes = currentInfo.fkIndexes;

		for(BaseEntity e : targetEntities) {
			entities = entities.removeValue(e);
			entityMap = entityMap.removeValue(e.id);
			
			newPkIndex = newPkIndex.delete(e);
			newFkIndexes = newFkIndexes.delete(e);
			
			currentInfo.deleteSet.add(e.id);
		}
		
		setInfo(currentInfo.copyDelta(entities, entityMap, newPkIndex, newFkIndexes));
	}
	
	private Set<AttributeValue> extractValues(Set<Pair<String, Object>> attributeValues) {
		return (Set<AttributeValue>) CollectionUtils.map(attributeValues, new TreeSet<AttributeValue>(), 
				new Function1<AttributeValue, Pair<String, Object>>() {
					@Override
					public AttributeValue execute(Pair<String, Object> p) {
						Attribute att = attributeFinder.execute(p.first);
						if(att == null) {
							throw new RuntimeException("Unknown attribute name: " + p.first);
						}
						if(! att.type.accepts(p.second)) {
							throw new ConstraintViolationException("Wrong type for " + p.first);
						}
						return new AttributeValue(att, p.second);
					}});
	}

	@Override
	public Set<Entity> select() {
		return Collections.<Entity>unmodifiableSet(getInfo().entities);
	}
	
//	private Entity extract(Entity e, Set<Attribute> attributes) {
//		Set<AttributeValue> pkValues = new TreeSet<AttributeValue>();
//		for(Attribute att : attributes) {
//			pkValues.add(e.getAttributeValue(att.name));
//		}
//		return new Entity(pkValues);
//	}

	protected Entity selectByKey(Entity ent) {
		OUTER: for(Entity e : getInfo().entities) {
			for(AttributeValue pkVal : ent.getValues()) {
				Object eval = e.getValue(pkVal.attribute.name);
				if(! ObjectUtils.equalsIfNull(pkVal.value, eval)) {
					continue OUTER;
				}
			}
			return e;
		}
		return null;
	}

	protected Entity selectByPk(Entity pk) {
		BaseRelationIndex pkIndex = getInfo().pkIndex;
		PersistentSet<BaseEntity> e = pkIndex.find(pk);
		if(e == null || e.isEmpty()) {
			return null;
		} else {
			Iterator<BaseEntity> ite = e.iterator();
			if(! ite.hasNext()) {
				throw new RuntimeException("Set is not empty but the iterator does not return elemens ?");
			}
			return ite.next();
		}
	}
	
	private BaseRelationInfo getInfo() {
		return domain.manager.get(this);
	}
	
	private void setInfo(BaseRelationInfo newInfo) {
		domain.manager.set(this, newInfo);
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

	// TODO checks are ineffective currently
	public void integrityChecks() {
		checkPrimaryKey();
		checkForeignKeys();
	}

	private void checkPrimaryKey() {
		PersistentSet<BaseEntity> entities = getInfo().entities;
		int pkIndexSize = getInfo().pkIndex.size();
		if(entities.size() != pkIndexSize) {
			throw new ConstraintViolationException(name + ": Primary key constraint violated");
		}
	}

	private void checkForeignKeys() {
		BaseRelationIndexes fkIndexes = getInfo().fkIndexes;
		for(Map.Entry<BaseRelation, Set<AttributeMapping>> rel : fks.entrySet()) {
			Set<Attribute> fkAttributes = extractAttributes(rel.getValue(), false);
			Set<Entity> fkIndex = fkIndexes.indexFor(fkAttributes).index();
			Set<Entity> pkIndex = rel.getKey().getPkIndex().index();
			
			for(Entity fk : fkIndex) {
				Entity pk = makeReferenceEntity(fk, rel.getValue());
				if(! pkIndex.contains(pk)) {
					throw new ConstraintViolationException(name + ": Foreign key to " + rel.getKey().name + " violated");
				}
			}
		}
	}
	
	private BaseRelationIndex getPkIndex() {
		return getInfo().pkIndex;
	}

	private Set<Attribute> extractAttributes(Set<AttributeMapping> mapping, final boolean pk) {
		return SetUtils.map(mapping, new Function1<Attribute, AttributeMapping>() {
			@Override
			public Attribute execute(AttributeMapping p) {
				if(pk) {
					return p.target;
				} else {
					return p.source;
				}
			}});
	}
	
//	private void checkForeignKeys() {
//		for(Entity e : getInfo().entities) {
//			for(Map.Entry<BaseRelation, Set<AttributeMapping>> rel : fks.entrySet()) {
//				Entity fkEntity = makeReferenceEntity(e, rel.getValue());
//				if(rel.getKey().selectByPk(fkEntity) == null) {
//					throw new ConstraintViolationException(name + ": Foreign key to " + rel.getKey().name + " violated");
//				}
//			}
//		}
//	}
	
	private Entity makeReferenceEntity(final Entity thisEntity, Set<AttributeMapping> mappings) {
		Set<AttributeValue> values = SetUtils.map(mappings, new Function1<AttributeValue, AttributeMapping>() {
			@Override
			public AttributeValue execute(AttributeMapping p) {
				AttributeValue val = thisEntity.getAttributeValue(p.source);
				return new AttributeValue(p.target, val.value);
			}});
		return new Entity(values);
	}

	protected BaseRelationInfo copyInfo(BaseRelationInfo value) {
		return value.clearCopy();
	}

}
