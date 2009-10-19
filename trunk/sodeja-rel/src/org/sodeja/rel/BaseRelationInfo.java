package org.sodeja.rel;

import java.util.UUID;

import org.sodeja.collections.PersistentSet;

class BaseRelationInfo {
	public final PersistentSet<BaseEntity> entities;
	
	public final PersistentSet<UUID> newSet;
	public final PersistentSet<UUID> changeSet;
	
	public BaseRelationInfo() {
		this(new PersistentSet<BaseEntity>(), new PersistentSet<UUID>(), new PersistentSet<UUID>());
	}
	
	public BaseRelationInfo(PersistentSet<BaseEntity> entities, PersistentSet<UUID> newSet, PersistentSet<UUID> changeSet) {
		this.entities = entities;
		this.newSet = newSet;
		this.changeSet = changeSet;
	}
}
