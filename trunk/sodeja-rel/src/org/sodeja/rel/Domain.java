package org.sodeja.rel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.sodeja.collections.SetUtils;
import org.sodeja.functional.Pair;
import org.sodeja.lang.StringUtils;

public class Domain {
	private final Set<BaseRelation> baseRelations = new HashSet<BaseRelation>();
	private final Map<String, Relation> relations = new HashMap<String, Relation>();
	private final Map<String, IntegrityCheck> checks = new HashMap<String, IntegrityCheck>();
	protected final TransactionManagerImpl manager = new TransactionManagerImpl(this);
	
	public Domain() {
	}
	
	public void addCheck(String name, IntegrityCheck cond) {
		checks.put(name, cond);
	}
	
	public void removeCheck(String name) {
		checks.remove(name);
	}
	
	protected void performExternalChecks() {
		for(Map.Entry<String, IntegrityCheck> check : checks.entrySet()) {
			if(! check.getValue().perform()) {
				throw new ConstraintViolationException("Integrity constraint \"" + check.getKey() + "\" failed");
			}
		}
	}
	
	protected void performInternalChecks() {
		for(BaseRelation rel : baseRelations) {
			rel.integrityChecks();
		}
	}
	
	public Type alias(Type type) {
		return new Alias(type);
	}
	
	public Type enumType(Class<? extends Enum<?>> enu) {
		return new EnumType(enu);
	}
	
	public Relation resolve(String name) {
		Relation rel = relations.get(name);
		if(rel == null) {
			throw new RuntimeException("Relation " + name + " not found in the domain");
		}
		return rel;
	}
	
	public BaseRelation resolveBase(String name) {
		Relation rel = resolve(name);
		if(! (rel instanceof BaseRelation)) {
			throw new RuntimeException("Relation " + name + " is not base in the domain");
		}
		return (BaseRelation) rel;
	}
	
	protected <T extends Relation> T remember(T relation) {
		return remember(relation.getName(), relation);
	}
	
	protected <T extends Relation> T remember(String name, T relation) {
		if(! StringUtils.isTrimmedEmpty(name)) {
			relations.put(name, relation);
			if(relation instanceof BaseRelation) {
				baseRelations.add((BaseRelation) relation);
			}
		}
		return relation;
	}
	
	public BaseRelation relationPlain(String name, Object... attributes) {
		if(attributes.length % 2 != 0) {
			throw new RuntimeException("Expected pairs into the array");
		}
		Attribute[] atts = new Attribute[attributes.length / 2];
		for(int i = 0; i < atts.length; i++) {
			atts[i] = new Attribute((String) attributes[i * 2], (Type) attributes[i * 2 + 1]);
		}
		return relation(name, atts);
	}
	
	public BaseRelation relation(String name, Attribute... attributes) {
		if(StringUtils.isTrimmedEmpty(name)) {
			throw new RuntimeException("Name is required for base relations");
		}
		return remember(name, new BaseRelation(this, name, attributes));
	}

	public TransactionManager txm() {
		return manager;
	}
	
	public void insertPlain(String name, Object... namedValues) {
		insert(name, SetUtils.namedValuesToSet(namedValues));
	}
	
	public void insert(String name, Set<Pair<String, Object>> attributeValues) {
		resolveBase(name).insert(attributeValues);
	}

	public Set<Entity> select(String name) {
		return resolve(name).select();
	}
	
	public void updatePlain(String name, Condition cond, Object... namedValues) {
		update(name, cond, SetUtils.namedValuesToSet(namedValues));
	}
	
	public void update(String name, Condition cond, Set<Pair<String, Object>> attributeValues) {
		resolveBase(name).update(cond, attributeValues);
	}
	
	public void deletePlain(String name, Object... namedValues) {
		delete(name, SetUtils.namedValuesToSet(namedValues));
	}

	public void delete(String name, Set<Pair<String, Object>> attributeValues) {
		resolveBase(name).delete(attributeValues);
	}

	public void delete(String name, Condition cond) {
		resolveBase(name).delete(cond);
	}

	protected void integrityCheck() {
		performInternalChecks();
		performExternalChecks();
	}
}
