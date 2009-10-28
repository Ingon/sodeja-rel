package org.sodeja.rel;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.sodeja.collections.ArrayUtils;
import org.sodeja.collections.CollectionUtils;
import org.sodeja.collections.PersistentSet;
import org.sodeja.collections.SetUtils;
import org.sodeja.functional.Function1;
import org.sodeja.functional.Pair;
import org.sodeja.functional.Predicate1;
import org.sodeja.lang.ObjectUtils;
import org.sodeja.lang.Range;

public class BaseRelation implements Relation, BaseRelationListener {
	protected final Domain domain;
	protected final String name;
	protected final Set<Attribute> attributes;
	protected final Map<String, Attribute> attributesMap;
	
	// TODO move in info
	protected Set<Attribute> pk;
	protected final Set<ForeignKey> fks = new HashSet<ForeignKey>();
	
	protected BaseRelation(Domain domain, String name, Attribute... attributes) {
		this.domain = domain;
		this.name = name;
		
		this.attributes = new TreeSet<Attribute>(Arrays.asList(attributes));
		this.attributesMap = CollectionUtils.mappedValues(this.attributes, new Function1<String, Attribute>() {
			@Override
			public String execute(Attribute p) {
				return p.name;
			}});
		
		setInfo(new BaseRelationInfo());
		
		BaseRelationInfo info = getInfo(); // By default whole attribute set is the PK
		pk = this.attributes;
		setInfo(info.copyDelta(info.entities, new BaseRelationIndex(pk), info.fkIndexes));
	}
	
	public BaseRelation primaryKey(String... attributeNames) {
		pk = resolveAttributes(attributeNames);
		BaseRelationInfo info = getInfo();
		setInfo(info.copyDelta(info.entities, new BaseRelationIndex(pk), info.fkIndexes));
		
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
		
		Set<AttributeMapping> mappings = new HashSet<AttributeMapping>();
		for(int i : Range.of(thisAttributes)) {
			Attribute thisAtt = attributesMap.get(thisAttributes[i]);
			fkIndexAttributes.add(thisAtt);
			
			Attribute foreignAtt = target.attributesMap.get(foreignAttributes[i]);
			targetPkCandidate.add(foreignAtt);
			
			mappings.add(new AttributeMapping(thisAtt, foreignAtt));
		}
		
		if(! target.pk.equals(targetPkCandidate)) {
			throw new ConstraintViolationException("Foreign key set is not refering to primary key");
		}
		
		fks.add(new ForeignKey(target, mappings));
		target.addListener(this);
		
		BaseRelationInfo info = getInfo();
		setInfo(info.copyDelta(info.entities, info.pkIndex, info.fkIndexes.addIndex(new BaseRelationIndex(fkIndexAttributes))));
		
		return this;
	}

	protected void addListener(BaseRelationListener l) {
		BaseRelationInfo info = getInfo();
		setInfo(info.addListener(l));
	}
	
	private Set<Attribute> resolveAttributes(String... attributeNames) {
		return resolveAttributes(SetUtils.asSet(attributeNames));
	}

	protected Set<Attribute> resolveAttributes(Set<String> attributeNames) {
		Set<Attribute> atts = new TreeSet<Attribute>();
		for(String name : attributeNames) {
			Attribute att = attributesMap.get(name);
			if(att == null) {
				throw new RuntimeException("Unknown attribute name: " + name);
			}
			atts.add(att);
		}
		return atts;
	}
	
	public void insert(Set<Pair<String, Object>> attributeValues) {
		Entity e = new Entity(extractValues(attributeValues, true));
		
		setInfo(getInfo().addEntity(e));
		
		checkForeignKeys(e);
		fireInserted(e);
	}

	private void fireInserted(Entity e) {
		for(BaseRelationListener l : getInfo().getListeners()) {
			l.inserted(this, e);
		}
	}
	
	private void removeFromStarving(Entity e) {
		BaseRelationInfo currentInfo = getInfo();
		for(Iterator<Map.Entry<ForeignKey, Set<Entity>>> ite = currentInfo.starvingEntities.entrySet().iterator(); ite.hasNext(); ) {
			Map.Entry<ForeignKey, Set<Entity>> en = ite.next();
			Set<Entity> starvingIds = en.getValue();
			if(starvingIds == null || starvingIds.isEmpty()) {
				continue;
			}
			if(starvingIds.remove(e) && starvingIds.isEmpty()) {
				ite.remove();
			}
		}
	}
	
	private void checkForeignKeys(Entity e) {
		BaseRelationInfo currentInfo = getInfo();
		for(ForeignKey fk : fks) {
			Set<Entity> pkIndex = fk.foreignRelation.getPkIndex().index();
			Entity pk = fk.toTargetPk(e);
			if (! (pk.onlyNulls() || pkIndex.contains(pk))) {
				Set<Entity> starvingIds = currentInfo.starvingEntities.get(fk);
				if(starvingIds == null) {
					starvingIds = new HashSet<Entity>();
					currentInfo.starvingEntities.put(fk, starvingIds);
				}
				starvingIds.add(e);
			}
		}
	}

	public void update(Set<Pair<String, Object>> attributeValues, Set<Pair<String, Object>> newAttributeValues) {
		final Set<AttributeValue> values = extractValues(attributeValues, false);
		SortedSet<AttributeValue> pkValues = CollectionUtils.filter(values, new TreeSet<AttributeValue>(), new Predicate1<AttributeValue>() {
			@Override
			public Boolean execute(AttributeValue p) {
				return pk.contains(p.attribute);
			}});
		if(pkValues.size() == pk.size()) {
			Entity pk = new Entity(pkValues);
			PersistentSet<Entity> entities = getInfo().pkIndex.find(pk);
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
		for(Entity e : getInfo().entities) {
			if(cond.satisfied(e)) {
				setInfo(getInfo().removeEntity(e));
				removeFromStarving(e);
				
				Entity ne = new Entity(merge(e, attributeValues));
				
				setInfo(getInfo().addEntity(ne));
				checkForeignKeys(ne);
				
				fireUpdated(e, ne);
			}
		}
	}
	
	private void fireUpdated(Entity oe, Entity e) {
		for(BaseRelationListener l : getInfo().getListeners()) {
			l.updated(this, oe, e);
		}
	}
	
	private void updateAll(PersistentSet<Entity> targetEntities, Set<Pair<String, Object>> attributeValues) {
		for(Entity e : targetEntities) {
			setInfo(getInfo().removeEntity(e));
			removeFromStarving(e);
			
			Entity ne = new Entity(merge(e, attributeValues));
			
			setInfo(getInfo().addEntity(ne));
			checkForeignKeys(ne);
			
			fireUpdated(e, ne);
		}
	}

	private SortedSet<AttributeValue> merge(Entity e, Set<Pair<String, Object>> attributeValues) {
		SortedSet<AttributeValue> newValues = new TreeSet<AttributeValue>();
		newValues.addAll(e.getValues());
		
		for(Pair<String, Object> value : attributeValues) {
			AttributeValue oldValue = e.getAttributeValue(value.first);
			if(oldValue == null) {
				throw new RuntimeException("Unknown attribute " + value.first);
			}
			
			Attribute att = oldValue.attribute;
			if(! att.type.accepts(value.second)) {
				throw new RuntimeException("Wrong type for attribute " + value.first);
			}
			
			AttributeValue newValue = new AttributeValue(oldValue.attribute, att.type.canonize(value.second));
			newValues.remove(oldValue);
			newValues.add(newValue);
		}
		
		return newValues;
	}

	public void delete(Set<Pair<String, Object>> attributeValues) {
		final Set<AttributeValue> values = extractValues(attributeValues, false);
		SortedSet<AttributeValue> pkValues = CollectionUtils.filter(values, new TreeSet<AttributeValue>(), new Predicate1<AttributeValue>() {
			@Override
			public Boolean execute(AttributeValue p) {
				return pk.contains(p.attribute);
			}});
		if(pkValues.size() == pk.size()) {
			Entity pk = new Entity(pkValues);
			PersistentSet<Entity> entities = getInfo().pkIndex.find(pk);
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
		PersistentSet<Entity> entities = getInfo().entities;
		for(Entity e : entities) {
			if(cond.satisfied(e)) {
				setInfo(getInfo().removeEntity(e));
				removeFromStarving(e);
				fireDeleted(e);
			}
		}
	}

	private void deleteAll(Set<Entity> targetEntities) {
		for(Entity e : targetEntities) {
			setInfo(getInfo().removeEntity(e));
			removeFromStarving(e);
			fireDeleted(e);
		}
	}
	
	private void fireDeleted(Entity e) {
		for(BaseRelationListener l : getInfo().getListeners()) {
			l.deleted(this, e);
		}
	}
	
	private SortedSet<AttributeValue> extractValues(Set<Pair<String, Object>> attributeValues, boolean strict) {
		if(strict && (attributeValues.size() != attributes.size())) {
			throw new RuntimeException("Expected values for all relation attributes");
		}
		
		SortedSet<AttributeValue> result = new TreeSet<AttributeValue>();
		for(Pair<String, Object> p : attributeValues) {
			Attribute att = attributesMap.get(p.first);
			if(att == null) {
				throw new RuntimeException("Unknown attribute name: " + p.first);
			}
			
			if(! att.type.accepts(p.second)) {
				throw new ConstraintViolationException("Wrong type for " + p.first);
			}
			
			result.add(new AttributeValue(att, att.type.canonize(p.second)));
		}
		return result;
		
//		return SetUtils.maps(attributeValues, new Function1<AttributeValue, Pair<String, Object>>() {
//				@Override
//				public AttributeValue execute(Pair<String, Object> p) {
//					Attribute att = attributeFinder.execute(p.first);
//					if(att == null) {
//						throw new RuntimeException("Unknown attribute name: " + p.first);
//					}
//					if(! att.type.accepts(p.second)) {
//						throw new ConstraintViolationException("Wrong type for " + p.first);
//					}
//					return new AttributeValue(att, att.type.canonize(p.second));
//				}});
	}

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
		PersistentSet<Entity> e = getInfo().pkIndex.find(pk);
		if(e == null || e.isEmpty()) {
			return null;
		}
		
		Iterator<Entity> ite = e.iterator();
		if(! ite.hasNext()) {
			throw new RuntimeException("Set is not empty but the iterator does not return elemens ?");
		}
		
		return ite.next();
	}
	
	private BaseRelationInfo getInfo() {
		return domain.manager.get(this);
	}
	
	private void setInfo(BaseRelationInfo newInfo) {
		domain.manager.set(this, newInfo);
	}
	
	// TODO checks are ineffective currently
	public void integrityChecks() {
		checkPrimaryKey();
		checkForeignKeys();
	}

	private void checkPrimaryKey() {
		PersistentSet<Entity> entities = getInfo().entities;
		int pkIndexSize = getInfo().pkIndex.size();
		if(entities.size() != pkIndexSize) {
			throw new ConstraintViolationException(name + ": Primary key constraint violated");
		}
	}

	private void checkForeignKeys() {
		BaseRelationInfo info = getInfo();
		if(! info.starvingEntities.isEmpty()) {
			for(Map.Entry<ForeignKey, Set<Entity>> e : info.starvingEntities.entrySet()) {
				throw new ConstraintViolationException(name + ": Foreign key to " + e.getKey().foreignRelation.name + " violated");
			}
		}
	}
	
	private BaseRelationIndex getPkIndex() {
		return getInfo().pkIndex;
	}

	protected BaseRelationInfo copyInfo(BaseRelationInfo value) {
		return value.clearCopy();
	}

	@Override
	public void inserted(BaseRelation relation, Entity entity) {
		BaseRelationInfo info = getInfo();
		
		for(ForeignKey fk : fks) {
			if(! fk.foreignRelation.equals(relation)) {
				continue;
			}
			
			Set<Entity> starvingIds = info.starvingEntities.get(fk);
			if(starvingIds == null || starvingIds.isEmpty()) {
				return;
			}
			
			Entity fkEntity = fk.toSourceFk(entity);
			for(Iterator<Entity> ite = starvingIds.iterator(); ite.hasNext(); ) {
				Entity l = ite.next();
				if(subsetOf(l, fkEntity)) {
					ite.remove();
				}
			}
			if(starvingIds.isEmpty()) {
				info.starvingEntities.remove(fk);
			}
		}
	}

	@Override
	public void updated(BaseRelation relation, Entity oldEntity, Entity newEntity) {
		deleted(relation, oldEntity);
		inserted(relation, newEntity);
	}

	@Override
	public void deleted(BaseRelation relation, Entity entity) {
		BaseRelationInfo info = getInfo();
		
		for(ForeignKey fk : fks) {
			if(! fk.foreignRelation.equals(relation)) {
				continue;
			}
			
			Set<Entity> starvingIds = info.starvingEntities.get(fk);
			BaseRelationIndex index = info.fkIndexes.indexFor(fk.getSourceAttributes());
			Entity fkOldEntity = fk.toSourceFk(entity);
			Set<Entity> oldEntities = index.find(fkOldEntity);
			if(oldEntities != null) {
				if(starvingIds == null) {
					starvingIds = new HashSet<Entity>();
					info.starvingEntities.put(fk, starvingIds);
				}
				
				for(Entity e : oldEntities) {
					starvingIds.add(e);
				}
			}
		}
	}

	private boolean subsetOf(Entity baseEntity, Entity fkEntity) {
		for(AttributeValue fv : fkEntity.values) {
			AttributeValue v = baseEntity.getAttributeValue(fv.attribute);
			if(! ObjectUtils.equalsIfNull(fv.value, v.value)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Set<Entity> select() {
		return Collections.<Entity>unmodifiableSet(getInfo().entities);
	}

	public Entity selectByPk(Set<Pair<String, Object>> pk) {
		Entity pke = new Entity(extractValues(pk, false));
		return selectByPk(pke);
	}
	
	public ForeignKey getFk(BaseRelation relation) {
		ForeignKey result = null;
		for(ForeignKey fk : fks) {
			if(fk.foreignRelation.equals(relation)) {
				if(result != null) {
					throw new RuntimeException("Ambiguous foreign key for " + relation.getName());
				}
				result = fk;
			}
		}
		return result;
	}
	
	public ForeignKey getFk(BaseRelation relation, String... attributeStrs) {
		Set<Attribute> attributes = resolveAttributes(attributeStrs);
		for(ForeignKey fk : fks) {
			if(fk.foreignRelation.equals(relation) && fk.getSourceAttributes().equals(attributes)) {
				return fk;
			}
		}
		return null;
	}
	
	public Set<AttributeMapping> getFkMapping(BaseRelation relation) {
		ForeignKey fk = getFk(relation);
		if(fk == null) {
			return null;
		}
		return fk.mappings;
	}
	
	public Set<AttributeMapping> getFkMapping(BaseRelation relation, String... attributeStrs) {
		ForeignKey fk = getFk(relation, attributeStrs);
		if(fk == null) {
			return null;
		}
		return fk.mappings;
	}
	
	public Set<ForeignKey> getFks() {
		return Collections.unmodifiableSet(fks);
	}
	
	public Set<Attribute> getAttributes() {
		return Collections.unmodifiableSet(attributes);
	}
}
