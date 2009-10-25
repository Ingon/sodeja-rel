package org.sodeja.rel.relations;

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.sodeja.collections.SetUtils;
import org.sodeja.functional.Function1;
import org.sodeja.rel.AttributeValue;
import org.sodeja.rel.Entity;
import org.sodeja.rel.ForeignKey;
import org.sodeja.rel.Relation;

public class RevealRelation implements Relation {
	protected final String name;
	protected final Relation base;	
	protected final Set<Reveal> reveals;
	
	public RevealRelation(String name, Relation base, Set<Reveal> reveals) {
		this.name = name;
		this.base = base;
		this.reveals = reveals;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Set<Entity> select() {
		Set<Entity> set = base.select();
		return SetUtils.map(set, new Function1<Entity, Entity>() {
			@Override
			public Entity execute(Entity p) {
				SortedSet<AttributeValue> newVals = new TreeSet<AttributeValue>(p.getValues());
				for(Reveal reveal : reveals) {
					Entity pke = reveal.fk.selectPk(p);
					for(String att : reveal.attributes) {
						AttributeValue v = pke.getAttributeValue(att);
						newVals.add(v.rename(reveal.prefix + v.attribute.name));
					}
				}
				return new Entity(newVals);
			}});
	}
	
	public RevealRelation copyOn(Relation newBase) {
		return new RevealRelation(name, newBase, reveals);
	}
	
	public static class Reveal {
		protected final ForeignKey fk;
		protected final String prefix;
		protected final Set<String> attributes;
		
		public Reveal(ForeignKey fk, String prefix, Set<String> attributes) {
			fk.validateTargetAttributes(attributes);
			
			this.fk = fk;
			this.prefix = prefix;
			this.attributes = attributes;
		}
	}
}
