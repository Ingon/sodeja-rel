package org.sodeja.rel;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.sodeja.collections.PersistentMap;
import org.sodeja.collections.PersistentSet;

class BaseRelationInfo {
	public final PersistentSet<BaseEntity> entities;
	public final PersistentMap<Long, BaseEntity> entityMap;
	
	public final BaseRelationIndex pkIndex;
	public final BaseRelationIndexes fkIndexes;
	
	public final Set<Long> newSet;
	public final Set<Long> updateSet;
	public final Set<Long> deleteSet;
	
	public BaseRelationInfo() {
		this(new PersistentSet<BaseEntity>(), new PersistentMap<Long, BaseEntity>(), 
				new BaseRelationIndex(new TreeSet<Attribute>()), new BaseRelationIndexes(),
				new TreeSet<Long>(), new TreeSet<Long>(), new TreeSet<Long>());
	}

	public BaseRelationInfo(PersistentSet<BaseEntity> entities, PersistentMap<Long, BaseEntity> entityMap, 
			BaseRelationIndex pkIndex, BaseRelationIndexes fkIndexes,
			Set<Long> newSet, Set<Long> updateSet, Set<Long> deleteSet) {

		this.entities = entities;
		this.entityMap = entityMap;
		
		this.pkIndex = pkIndex;
		this.fkIndexes = fkIndexes;
		
		this.newSet = newSet;
		this.updateSet = updateSet;
		this.deleteSet = deleteSet;
	}
	
	public boolean hasChanges() {
		return ! (newSet.isEmpty() && updateSet.isEmpty() && deleteSet.isEmpty());
	}
	
	public BaseRelationInfo copyDelta(PersistentSet<BaseEntity> entities, PersistentMap<Long, BaseEntity> entityMap, 
			BaseRelationIndex pkIndex, BaseRelationIndexes fkIndexes) {
		return new BaseRelationInfo(entities, entityMap, pkIndex, fkIndexes, this.newSet, this.updateSet, this.deleteSet);
	}

	public BaseRelationInfo clearCopy() {
		return new BaseRelationInfo(entities, entityMap, pkIndex, fkIndexes, new TreeSet<Long>(), new TreeSet<Long>(), new TreeSet<Long>());
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
		BaseRelationIndex newPkIndex = pkIndex;
		BaseRelationIndexes newFkIndexes = fkIndexes;
		
		for(Long id : versionInfo.newSet) {
			BaseEntity e = versionInfo.entityMap.get(id);
			
			newEntities = newEntities.addValue(e);
			newEntityMap = newEntityMap.putValue(id, e);
			
			newPkIndex = newPkIndex.insert(e);
			newFkIndexes = newFkIndexes.insert(e);
		}
		
		for(Long id : versionInfo.updateSet) {
			BaseEntity ne = versionInfo.entityMap.get(id);
			BaseEntity oe = newEntityMap.get(id);
			
			newEntities = newEntities.removeValue(oe);
			
			newPkIndex = newPkIndex.delete(oe);
			newFkIndexes = newFkIndexes.delete(oe);
			
			newEntities = newEntities.addValue(ne);
			newEntityMap = newEntityMap.putValue(id, ne);
			
			newPkIndex = newPkIndex.insert(ne);
			newFkIndexes = newFkIndexes.insert(ne);
		}
		
		for(Long id : versionInfo.deleteSet) {
			BaseEntity oe = newEntityMap.get(id);
			if(oe == null) {
				continue;
			}
			
			newEntities = newEntities.removeValue(oe);
			newEntityMap = newEntityMap.removeValue(id);
			
			newPkIndex = newPkIndex.delete(oe);
			newFkIndexes = newFkIndexes.delete(oe);
		}
		
		return new BaseRelationInfo(newEntities, newEntityMap, newPkIndex, newFkIndexes, this.newSet, this.updateSet, this.deleteSet);
	}
}
