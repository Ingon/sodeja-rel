package org.sodeja.rel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.sodeja.collections.PersistentSet;

class BaseRelationInfo {
	public final PersistentSet<Entity> entities;
	
	public final BaseRelationIndex pkIndex;
	public final BaseRelationIndexes fkIndexes;
	
	public final Set<Entity> newSet;
	public final Set<Entity> deleteSet;
	
	public final Map<ForeignKey, Set<Entity>> starvingEntities; 
	
	public BaseRelationInfo() {
		this(new PersistentSet<Entity>(),
				new BaseRelationIndex(new TreeSet<Attribute>()), new BaseRelationIndexes(),
				new HashSet<Entity>(), new HashSet<Entity>(), new HashMap<ForeignKey, Set<Entity>>());
	}

	public BaseRelationInfo(PersistentSet<Entity> entities, 
			BaseRelationIndex pkIndex, BaseRelationIndexes fkIndexes,
			Set<Entity> newSet, Set<Entity> deleteSet, Map<ForeignKey, Set<Entity>> starvedEntities) {

		this.entities = entities;
		
		this.pkIndex = pkIndex;
		this.fkIndexes = fkIndexes;
		
		this.newSet = newSet;
		this.deleteSet = deleteSet;
		
		this.starvingEntities = starvedEntities;
	}
	
	public boolean hasChanges() {
		return ! (newSet.isEmpty() && deleteSet.isEmpty());
	}
	
	public BaseRelationInfo copyDelta(PersistentSet<Entity> entities, BaseRelationIndex pkIndex, BaseRelationIndexes fkIndexes) {
		return new BaseRelationInfo(entities, pkIndex, fkIndexes, this.newSet, this.deleteSet, this.starvingEntities);
	}

	public BaseRelationInfo clearCopy() {
		return new BaseRelationInfo(entities, pkIndex, fkIndexes, 
				new HashSet<Entity>(), new HashSet<Entity>(), new HashMap<ForeignKey, Set<Entity>>());
	}
	
//	protected Set<Entity> updateSet() {
//		Set<Entity> set = new HashSet<Entity>();
//		set.addAll(newSet);
//		return set;
//	}
//	
//	protected Set<Entity> changeSet() {
//		Set<Entity> set = new HashSet<Entity>();
//		set.addAll(deleteSet);
//		return set;
//	}

	protected BaseRelationInfo merge(BaseRelationInfo versionInfo) {
		PersistentSet<Entity> newEntities = entities;
		BaseRelationIndex newPkIndex = pkIndex;
		BaseRelationIndexes newFkIndexes = fkIndexes;
		
		for(Entity e : versionInfo.newSet) {
			newEntities = newEntities.addValue(e);
			
			newPkIndex = newPkIndex.insert(e);
			newFkIndexes = newFkIndexes.insert(e);
		}
		
		for(Entity oe : versionInfo.deleteSet) {
			newEntities = newEntities.removeValue(oe);
			
			newPkIndex = newPkIndex.delete(oe);
			newFkIndexes = newFkIndexes.delete(oe);
		}
		
		return new BaseRelationInfo(newEntities, newPkIndex, newFkIndexes, 
				this.newSet, this.deleteSet, this.starvingEntities);
	}
}
