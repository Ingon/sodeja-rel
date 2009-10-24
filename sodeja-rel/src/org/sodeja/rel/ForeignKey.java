package org.sodeja.rel;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.sodeja.collections.CollectionUtils;
import org.sodeja.functional.Function1;

class ForeignKey {
	protected final BaseRelation foreignRelation;
	protected final Set<AttributeMapping> mappings;
	
	public ForeignKey(BaseRelation foreignRelation, Set<AttributeMapping> mappings) {
		this.foreignRelation = foreignRelation;
		this.mappings = Collections.unmodifiableSet(mappings);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + foreignRelation.hashCode();
		result = prime * result + mappings.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(! (obj instanceof ForeignKey)) {
			return false;
		}
		
		ForeignKey other = (ForeignKey) obj;
		return this.foreignRelation.equals(other.foreignRelation) &&
			this.mappings.equals(other.mappings);
	}
	
	public Set<Attribute> getSourceAttributes() {
		return CollectionUtils.map(mappings, new TreeSet<Attribute>(), new Function1<Attribute, AttributeMapping>() {
			@Override
			public Attribute execute(AttributeMapping p) {
				return p.source;
			}});
	}
}
