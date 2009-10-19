package org.sodeja.rel;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.sodeja.collections.PersistentMap;
import org.sodeja.collections.PersistentSet;

class BaseRelationInfo {
	public final PersistentSet<BaseEntity> entities;
	public final PersistentMap<Long, BaseEntity> entityMap;
	
	public final Set<Long> newSet;
	public final Set<Long> updateSet;
	public final Set<Long> deleteSet;
	
	public BaseRelationInfo() {
		this(new PersistentSet<BaseEntity>(), new PersistentMap<Long, BaseEntity>(), new TreeSet<Long>(), new TreeSet<Long>(), new TreeSet<Long>());
	}

	public BaseRelationInfo(PersistentSet<BaseEntity> entities, PersistentMap<Long, BaseEntity> entityMap,  
			Set<Long> newSet, Set<Long> updateSet, Set<Long> deleteSet) {

		this.entities = entities;
		this.entityMap = entityMap;
		
		this.newSet = newSet;
		this.updateSet = updateSet;
		this.deleteSet = deleteSet;
	}
	
	public boolean hasChanges() {
		return ! (newSet.isEmpty() && updateSet.isEmpty() && deleteSet.isEmpty());
	}
	
	public BaseRelationInfo copyDelta(PersistentSet<BaseEntity> entities, PersistentMap<Long, BaseEntity> entityMap) {
		return new BaseRelationInfo(entities, entityMap, this.newSet, this.updateSet, this.deleteSet);
	}

	public BaseRelationInfo clearCopy() {
		return new BaseRelationInfo(entities, entityMap, new TreeSet<Long>(), new TreeSet<Long>(), new TreeSet<Long>());
	}
	
	protected Set<Long> changeSet() {
		Set<Long> set = new HashSet<Long>();
		set.addAll(updateSet);
		set.addAll(deleteSet);
		return set;
	}

	protected BaseRelationInfo merge(BaseRelationInfo versionInfo) {
		PersistentSet<BaseEntity> newEntities = entities;
		PersistentMap<Long, BaseEntity> newEntityMap = entityMap;
		
		for(Long id : versionInfo.newSet) {
			BaseEntity e = versionInfo.entityMap.get(id);
			
			newEntities = newEntities.addValue(e);
			newEntityMap = newEntityMap.putValue(id, e);
		}
		
		for(Long id : versionInfo.updateSet) {
			BaseEntity ne = versionInfo.entityMap.get(id);
			BaseEntity oe = newEntityMap.get(id);
			
			newEntities = newEntities.removeValue(oe);
			newEntities = newEntities.addValue(ne);
			newEntityMap = newEntityMap.putValue(id, ne);
		}
		
		for(Long id : versionInfo.deleteSet) {
			BaseEntity oe = newEntityMap.get(id);
			if(oe == null) {
				continue;
			}
			newEntities = newEntities.removeValue(oe);
			newEntityMap = newEntityMap.removeValue(id);
		}
		
		return new BaseRelationInfo(newEntities, newEntityMap, this.newSet, this.updateSet, this.deleteSet);
	}
}
