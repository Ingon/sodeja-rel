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
	
	public final PersistentSet<BaseRelationListener> listeners;
	
	public final Set<Entity> newSet;
	public final Set<Entity> deleteSet;
	
	public final Map<ForeignKey, Set<Entity>> starvingEntities; 
	
	public BaseRelationInfo() {
		this(new PersistentSet<Entity>(),
				new BaseRelationIndex(new TreeSet<Attribute>()), new BaseRelationIndexes(),
				new PersistentSet<BaseRelationListener>(),
				new HashSet<Entity>(), new HashSet<Entity>(), 
				new HashMap<ForeignKey, Set<Entity>>());
	}

	public BaseRelationInfo(PersistentSet<Entity> entities, 
			BaseRelationIndex pkIndex, BaseRelationIndexes fkIndexes,
			PersistentSet<BaseRelationListener> listeners,
			Set<Entity> newSet, Set<Entity> deleteSet, 
			Map<ForeignKey, Set<Entity>> starvedEntities) {

		this.entities = entities;
		
		this.pkIndex = pkIndex;
		this.fkIndexes = fkIndexes;
		
		this.listeners = listeners;
		
		this.newSet = newSet;
		this.deleteSet = deleteSet;
		
		this.starvingEntities = starvedEntities;
	}
	
	public boolean hasChanges() {
		return ! (newSet.isEmpty() && deleteSet.isEmpty());
	}
	
	protected BaseRelationInfo copyDelta(PersistentSet<Entity> entities, BaseRelationIndex pkIndex, BaseRelationIndexes fkIndexes) {
		return new BaseRelationInfo(entities, pkIndex, fkIndexes, this.listeners, 
				this.newSet, this.deleteSet, this.starvingEntities);
	}

	public BaseRelationInfo clearCopy() {
		return new BaseRelationInfo(entities, pkIndex, fkIndexes, this.listeners, 
				new HashSet<Entity>(), new HashSet<Entity>(), new HashMap<ForeignKey, Set<Entity>>());
	}

	public BaseRelationInfo addListener(BaseRelationListener l) {
		return new BaseRelationInfo(this.entities, this.pkIndex, this.fkIndexes, this.listeners.addValue(l), 
				this.newSet, this.deleteSet, this.starvingEntities);
	}
	
	public BaseRelationInfo addEntity(Entity e) {
		PersistentSet<Entity> newEntities = entities.addValue(e);
		
		BaseRelationIndex newPkIndex = pkIndex.insert(e);
		BaseRelationIndexes newFkIndexes = fkIndexes.insert(e);
		
		newSet.add(e);
		
		return copyDelta(newEntities, newPkIndex, newFkIndexes);
	}
	
	public BaseRelationInfo removeEntity(Entity e) {
		PersistentSet<Entity> newEntities = entities.removeValue(e);
		
		BaseRelationIndex newPkIndex = pkIndex.delete(e);
		BaseRelationIndexes newFkIndexes = fkIndexes.delete(e);
		
		deleteSet.add(e);
		
		return copyDelta(newEntities, newPkIndex, newFkIndexes);
	}
	
	protected BaseRelationInfo merge(BaseRelation relation, BaseRelationInfo versionInfo) {
		PersistentSet<Entity> newEntities = entities;
		BaseRelationIndex newPkIndex = pkIndex;
		BaseRelationIndexes newFkIndexes = fkIndexes;
		
		for(Entity e : versionInfo.newSet) {
			newEntities = newEntities.addValue(e);
			
			newPkIndex = newPkIndex.insert(e);
			newFkIndexes = newFkIndexes.insert(e);
			
			for(BaseRelationListener l : listeners) {
				l.inserted(relation, e);
			}
		}
		
		for(Entity oe : versionInfo.deleteSet) {
			newEntities = newEntities.removeValue(oe);
			
			newPkIndex = newPkIndex.delete(oe);
			newFkIndexes = newFkIndexes.delete(oe);
			
			for(BaseRelationListener l : listeners) {
				l.deleted(relation, oe);
			}
		}
		
		return new BaseRelationInfo(newEntities, newPkIndex, newFkIndexes, this.listeners, 
				this.newSet, this.deleteSet, this.starvingEntities);
	}
}
