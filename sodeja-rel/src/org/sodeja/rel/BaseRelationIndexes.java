package org.sodeja.rel;

import java.util.Set;

import org.sodeja.collections.PersistentSet;

public class BaseRelationIndexes {
	public final PersistentSet<BaseRelationIndex> indexes;

	protected BaseRelationIndexes() {
		this.indexes = new PersistentSet<BaseRelationIndex>();
	}
	
	private BaseRelationIndexes(PersistentSet<BaseRelationIndex> fkIndexes) {
		this.indexes = fkIndexes;
	}

	public BaseRelationIndexes insert(BaseEntity val) {
		PersistentSet<BaseRelationIndex> newFkIndexes = indexes;
		for(BaseRelationIndex index : newFkIndexes) {
			newFkIndexes = newFkIndexes.addValue(index.insert(val));
		}
		return new BaseRelationIndexes(newFkIndexes);
	}

	public BaseRelationIndexes delete(BaseEntity val) {
		PersistentSet<BaseRelationIndex> newFkIndexes = indexes;
		for(BaseRelationIndex index : newFkIndexes) {
			newFkIndexes = newFkIndexes.addValue(index.delete(val));
		}
		return new BaseRelationIndexes(newFkIndexes);
	}
	
	public BaseRelationIndexes addIndex(BaseRelationIndex index) {
		if(indexes.contains(index)) {
			return this;
		}
		return new BaseRelationIndexes(indexes.addValue(index));
	}

	public BaseRelationIndexes removeIndex(BaseRelationIndex index) {
		return new BaseRelationIndexes(indexes.removeValue(index));
	}
	
	public BaseRelationIndex indexFor(Set<Attribute> attributes) { // TODO could be cached with PersistentMap
		for(BaseRelationIndex index : indexes) {
			if(index.attributes.equals(attributes)) {
				return index;
			}
		}
		throw new RuntimeException("Attributes should be indexed for sure");
	}
}
