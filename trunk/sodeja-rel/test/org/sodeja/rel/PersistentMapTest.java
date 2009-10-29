package org.sodeja.rel;

import java.util.HashMap;

import org.sodeja.collections.PersistentMap;
import org.sodeja.collections.SetUtils;

public class PersistentMapTest {
	public static void main(String[] args) {
		Attribute id = new Attribute("id", Types.LONG);
		Attribute name = new Attribute("name", Types.STRING);
		Attribute desc = new Attribute("description", Types.STRING);
		
		AttributeValue nameValue = new AttributeValue(name, "fairlyLongName");
		AttributeValue descValue = new AttributeValue(desc, "fairlyLongManagerName");
		
		PersistentMap<Entity, Boolean> map = new PersistentMap<Entity, Boolean>();
		long begin = System.currentTimeMillis();
		for(int i = 0; i < 200000; i++) {
			map = map.putValue(new Entity(SetUtils.asSets(new AttributeValue(id, i), nameValue, descValue)), Boolean.TRUE);
		}
		long end = System.currentTimeMillis();
		System.out.println("PTOTAL: " + (end - begin));

		HashMap<Entity, Boolean> map1 = new HashMap<Entity, Boolean>();
		begin = System.currentTimeMillis();
		for(int i = 0; i < 200000; i++) {
			map1.put(new Entity(SetUtils.asSets(new AttributeValue(id, i), nameValue, descValue)), Boolean.TRUE);
		}
		end = System.currentTimeMillis();
		System.out.println("HTOTAL: " + (end - begin));
	}
}
