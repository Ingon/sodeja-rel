package org.sodeja.rel;

import java.util.HashSet;
import java.util.Set;

import org.sodeja.collections.PersistentMap;
import org.sodeja.collections.PersistentSet;

class BaseRelationInfo {
	public final PersistentSet<BaseEntity> entities;
	public final PersistentMap<UUID, BaseEntity> entityMap;
	
	public final PersistentSet<UUID> newSet;
	public final PersistentSet<UUID> updateSet;
	public final PersistentSet<UUID> deleteSet;
	
	public BaseRelationInfo() {
		this(new PersistentSet<BaseEntity>(), new PersistentMap<UUID, BaseEntity>(), new PersistentSet<UUID>(), new PersistentSet<UUID>(), new PersistentSet<UUID>());
	}

	public BaseRelationInfo(PersistentSet<BaseEntity> entities, PersistentMap<UUID, BaseEntity> entityMap,  
			PersistentSet<UUID> newSet, PersistentSet<UUID> updateSet, PersistentSet<UUID> deleteSet) {

		this.entities = entities;
		this.entityMap = entityMap;
		
		this.newSet = newSet;
		this.updateSet = updateSet;
		this.deleteSet = deleteSet;
	}
	
	public boolean hasChanges() {
		return ! (newSet.isEmpty() && updateSet.isEmpty() && deleteSet.isEmpty());
	}
	
	public BaseRelationInfo newData(PersistentSet<BaseEntity> entities, PersistentMap<UUID, BaseEntity> entityMap, PersistentSet<UUID> newSet) {
		return new BaseRelationInfo(entities, entityMap, newSet, this.updateSet, this.deleteSet);
	}

	public BaseRelationInfo updateData(PersistentSet<BaseEntity> entities, PersistentMap<UUID, BaseEntity> entityMap, PersistentSet<UUID> updateSet) {
		return new BaseRelationInfo(entities, entityMap, this.newSet, updateSet, this.deleteSet);
	}

	public BaseRelationInfo deleteData(PersistentSet<BaseEntity> entities, PersistentMap<UUID, BaseEntity> entityMap, PersistentSet<UUID> deleteSet) {
		return new BaseRelationInfo(entities, entityMap, this.newSet, this.updateSet, deleteSet);
	}
	
	public BaseRelationInfo clearCopy() {
		return new BaseRelationInfo(entities, entityMap, new PersistentSet<UUID>(), new PersistentSet<UUID>(), new PersistentSet<UUID>());
	}
	
	protected Set<UUID> changeSet() {
		Set<UUID> set = new HashSet<UUID>();
		set.addAll(updateSet);
		set.addAll(deleteSet);
		return set;
	}

	protected BaseRelationInfo merge(BaseRelationInfo versionInfo) {
		PersistentSet<BaseEntity> newEntities = entities;
		PersistentMap<UUID, BaseEntity> newEntityMap = entityMap;
		
		for(UUID id : versionInfo.newSet) {
			BaseEntity e = versionInfo.entityMap.get(id);
			
			newEntities = newEntities.addValue(e);
			newEntityMap = newEntityMap.putValue(id, e);
		}
		
		for(UUID id : versionInfo.updateSet) {
			BaseEntity ne = versionInfo.entityMap.get(id);
			BaseEntity oe = newEntityMap.get(id);
			
			newEntities = newEntities.removeValue(oe);
			newEntities = newEntities.addValue(ne);
			newEntityMap = newEntityMap.putValue(id, ne);
		}
		
		for(UUID id : versionInfo.deleteSet) {
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
