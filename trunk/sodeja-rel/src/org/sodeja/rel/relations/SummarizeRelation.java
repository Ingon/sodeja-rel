package org.sodeja.rel.relations;

import org.sodeja.rel.Aggregate;
import org.sodeja.rel.Relation;

public class SummarizeRelation extends DerivedRelation {
	protected final Relation other;
	protected final Aggregate[] aggregates;
	
	public SummarizeRelation(String name, Relation relation, Relation other, Aggregate... aggregates) {
		super(name, relation);
		this.other = other;
		this.aggregates = aggregates;
	}
}
