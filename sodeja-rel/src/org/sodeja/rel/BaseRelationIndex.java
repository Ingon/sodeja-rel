package org.sodeja.rel;

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.sodeja.collections.PersistentMap;
import org.sodeja.collections.PersistentSet;

class BaseRelationIndex {
	public final Set<Attribute> attributes;
	public final PersistentMap<Entity, PersistentSet<BaseEntity>> index;
	
	public BaseRelationIndex(Set<Attribute> attributes) {
		this.attributes = attributes;
		this.index = new PersistentMap<Entity, PersistentSet<BaseEntity>>();
	}
	
	public BaseRelationIndex(Set<Attribute> attributes, PersistentMap<Entity, PersistentSet<BaseEntity>> index) {
		this.attributes = attributes;
		this.index = index;
	}
	
	public BaseRelationIndex insert(BaseEntity val) {
		if(attributes.isEmpty()) {
			return this;
		}
		
		Entity valIndex = extract(val);
		
		PersistentSet<BaseEntity> entitesForIndex = index.get(valIndex);
		if(entitesForIndex == null) {
			entitesForIndex = new PersistentSet<BaseEntity>();
		}
		PersistentSet<BaseEntity> newEntitiesForIndex = entitesForIndex.addValue(val);
		PersistentMap<Entity, PersistentSet<BaseEntity>> newIndex = index.putValue(valIndex, newEntitiesForIndex);
		
		return new BaseRelationIndex(attributes, newIndex);
	}

	public BaseRelationIndex delete(BaseEntity val) {
		if(attributes.isEmpty()) {
			return this;
		}
		
		Entity valIndex = extract(val);
		
		PersistentSet<BaseEntity> entitesForIndex = index.get(valIndex);
		if(entitesForIndex == null) {
			throw new RuntimeException("An unindexed value ?!?!?");
		}
		
		PersistentSet<BaseEntity> newEntitiesForIndex = entitesForIndex.removeValue(val);
		PersistentMap<Entity, PersistentSet<BaseEntity>> newIndex = index;
		if(newEntitiesForIndex.isEmpty()) {
			newIndex = index.removeValue(valIndex);
		} else {
			newIndex = index.putValue(valIndex, newEntitiesForIndex);
		}
		
		return new BaseRelationIndex(attributes, newIndex);
	}
	
	public int size() {
		return index.size();
	}
	
	private Entity extract(Entity e) {
		SortedSet<AttributeValue> pkValues = new TreeSet<AttributeValue>();
		for(Attribute att : attributes) {
			pkValues.add(e.getAttributeValue(att.name));
		}
		return new Entity(pkValues);
	}

	public PersistentSet<BaseEntity> find(Entity pk) {
		return index.get(pk);
	}
	
	public Set<Entity> index() {
		return index.keySet();
	}

	@Override
	public boolean equals(Object obj) {
		if(! (obj instanceof BaseRelationIndex)) {
			return false;
		}
		return this.attributes.equals(((BaseRelationIndex) obj).attributes);
	}

	@Override
	public int hashCode() {
		return this.attributes.hashCode();
	}
}
