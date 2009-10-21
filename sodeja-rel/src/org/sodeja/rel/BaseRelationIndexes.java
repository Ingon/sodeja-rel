package org.sodeja.rel;

import java.util.Set;

import org.sodeja.collections.PersistentSet;

public class BaseRelationIndexes {
	public final PersistentSet<BaseRelationIndex> fkIndexes;

	protected BaseRelationIndexes() {
		this.fkIndexes = new PersistentSet<BaseRelationIndex>();
	}
	
	private BaseRelationIndexes(PersistentSet<BaseRelationIndex> fkIndexes) {
		this.fkIndexes = fkIndexes;
	}

	public BaseRelationIndexes insert(BaseEntity val) {
		PersistentSet<BaseRelationIndex> newFkIndexes = fkIndexes;
		for(BaseRelationIndex index : newFkIndexes) {
			newFkIndexes = newFkIndexes.addValue(index.insert(val));
		}
		return new BaseRelationIndexes(newFkIndexes);
	}

	public BaseRelationIndexes delete(BaseEntity val) {
		PersistentSet<BaseRelationIndex> newFkIndexes = fkIndexes;
		for(BaseRelationIndex index : newFkIndexes) {
			newFkIndexes = newFkIndexes.addValue(index.delete(val));
		}
		return new BaseRelationIndexes(newFkIndexes);
	}
	
	public BaseRelationIndexes addIndex(BaseRelationIndex fkIndex) {
		if(fkIndexes.contains(fkIndex)) {
			return this;
		}
		return new BaseRelationIndexes(fkIndexes.addValue(fkIndex));
	}

	public BaseRelationIndexes removeIndex(BaseRelationIndex fkIndex) {
		return new BaseRelationIndexes(fkIndexes.removeValue(fkIndex));
	}
	
	public BaseRelationIndex indexFor(Set<Attribute> attributes) { // TODO could be cached with PersistentMap
		for(BaseRelationIndex index : fkIndexes) {
			if(index.attributes.equals(attributes)) {
				return index;
			}
		}
		throw new RuntimeException("Attributes should be indexed for sure");
	}
}
