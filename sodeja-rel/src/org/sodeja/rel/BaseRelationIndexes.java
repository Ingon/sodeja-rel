package org.sodeja.rel;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BaseRelationIndexes {
	private final Map<Set<Attribute>, BaseRelationIndex> indexes;

	protected BaseRelationIndexes() {
		this(new HashMap<Set<Attribute>, BaseRelationIndex>());
	}
	
	private BaseRelationIndexes(Map<Set<Attribute>, BaseRelationIndex> indexes) {
		this.indexes = Collections.unmodifiableMap(indexes);
	}

	public BaseRelationIndexes insert(Entity val) {
		Map<Set<Attribute>, BaseRelationIndex> newIndexes = new HashMap<Set<Attribute>, BaseRelationIndex>();
		for(Map.Entry<Set<Attribute>, BaseRelationIndex> index : indexes.entrySet()) {
			newIndexes.put(index.getKey(), index.getValue().insert(val));
		}
		return new BaseRelationIndexes(newIndexes);
	}

	public BaseRelationIndexes delete(Entity val) {
		Map<Set<Attribute>, BaseRelationIndex> newIndexes = new HashMap<Set<Attribute>, BaseRelationIndex>();
		for(Map.Entry<Set<Attribute>, BaseRelationIndex> index : indexes.entrySet()) {
			newIndexes.put(index.getKey(), index.getValue().delete(val));
		}
		return new BaseRelationIndexes(newIndexes);
	}
	
	public BaseRelationIndexes addIndex(BaseRelationIndex index) {
		if(indexes.containsKey(index.attributes)) {
			return this;
		}
		
		Map<Set<Attribute>, BaseRelationIndex> newIndexes = new HashMap<Set<Attribute>, BaseRelationIndex>(indexes);
		newIndexes.put(index.attributes, index);
		return new BaseRelationIndexes(newIndexes);
	}

	public BaseRelationIndexes removeIndex(BaseRelationIndex index) {
		Map<Set<Attribute>, BaseRelationIndex> newIndexes = new HashMap<Set<Attribute>, BaseRelationIndex>(indexes);
		newIndexes.remove(index.attributes);
		return new BaseRelationIndexes(newIndexes);
	}
	
	public BaseRelationIndex indexFor(Set<Attribute> attributes) { // TODO could be cached with PersistentMap
		return indexes.get(attributes);
	}
}
