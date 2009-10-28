package org.sodeja.rel;

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.sodeja.collections.CollectionUtils;
import org.sodeja.collections.SetUtils;
import org.sodeja.functional.Function1;

public class ForeignKey {
	protected final BaseRelation foreignRelation;
	protected final Set<AttributeMapping> mappings;
	private final Set<Attribute> sourceAttributesCache;
	private final Set<Attribute> targetAttributesCache;
	
	public ForeignKey(BaseRelation foreignRelation, Set<AttributeMapping> mappings) {
		this.foreignRelation = foreignRelation;
		this.mappings = Collections.unmodifiableSet(mappings);
		
		this.sourceAttributesCache = CollectionUtils.map(mappings, new TreeSet<Attribute>(), new Function1<Attribute, AttributeMapping>() {
			@Override
			public Attribute execute(AttributeMapping p) {
				return p.source;
			}});
		this.targetAttributesCache = CollectionUtils.map(mappings, new TreeSet<Attribute>(), new Function1<Attribute, AttributeMapping>() {
			@Override
			public Attribute execute(AttributeMapping p) {
				return p.target;
			}});
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
		return sourceAttributesCache;
//		return CollectionUtils.map(mappings, new TreeSet<Attribute>(), new Function1<Attribute, AttributeMapping>() {
//			@Override
//			public Attribute execute(AttributeMapping p) {
//				return p.source;
//			}});
	}

	public Set<Attribute> getTargetAttributes() {
		return targetAttributesCache;
//		return CollectionUtils.map(mappings, new TreeSet<Attribute>(), new Function1<Attribute, AttributeMapping>() {
//			@Override
//			public Attribute execute(AttributeMapping p) {
//				return p.target;
//			}});
	}

	public Entity toTargetPk(final Entity source) {
		SortedSet<AttributeValue> values = SetUtils.maps(mappings, new Function1<AttributeValue, AttributeMapping>() {
			@Override
			public AttributeValue execute(AttributeMapping p) {
				AttributeValue val = source.getAttributeValue(p.source);
				return new AttributeValue(p.target, val.value);
			}});
		
		return new Entity(values);
	}

	public Entity toSourceFk(final Entity target) {
		SortedSet<AttributeValue> values = SetUtils.maps(mappings, new Function1<AttributeValue, AttributeMapping>() {
			@Override
			public AttributeValue execute(AttributeMapping p) {
				AttributeValue val = target.getAttributeValue(p.target);
				return new AttributeValue(p.source, val.value);
			}});
		
		return new Entity(values);
	}

	public Entity selectPk(Entity source) {
		Entity targetPk = toTargetPk(source);
		if(targetPk.onlyNulls()) {
			return new Entity(SetUtils.maps(foreignRelation.attributes, new Function1<AttributeValue, Attribute>() {
				@Override
				public AttributeValue execute(Attribute p) {
					return new AttributeValue(p, null);
				}}));
		}
		return foreignRelation.selectByPk(targetPk);
	}
	
	public Set<Entity> select() {
		return foreignRelation.select();
	}
	
	public Set<Entity> selectResolved() {
		Relation resolvedRelation = foreignRelation.domain.resolveUnknown(foreignRelation.getName() + "Resolved");
		if(resolvedRelation != null) {
			return resolvedRelation.select();
		}
		return select();
	}
	
	public void validateTargetAttributes(Set<String> attributes) {
		foreignRelation.resolveAttributes(attributes);
	}
	
	public Set<AttributeMapping> invertedMapping() {
		return SetUtils.maps(mappings, new Function1<AttributeMapping, AttributeMapping>() {
			@Override
			public AttributeMapping execute(AttributeMapping p) {
				return new AttributeMapping(p.target, p.source);
			}});
	}
}
