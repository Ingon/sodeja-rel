package org.sodeja.rel;

import java.util.Set;

import org.sodeja.functional.Pair;

public interface BaseRelation extends Relation {
	public void addAttribute(String name, Type type);
	public void removeAttribute(String name);
	
	public void setPrimaryKey(Set<String> pkAttributes);
	public void addForeignKey(BaseRelation other, Set<Pair<String, String>> fkToPkAttributes);
	
	public void insert();
	public void update();
	public void delete();
}
