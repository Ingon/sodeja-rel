package org.sodeja.rel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.sodeja.collections.SetUtils;
import org.sodeja.functional.Pair;
import org.sodeja.lang.StringUtils;
import org.sodeja.rel.relations.ExtendRelation;
import org.sodeja.rel.relations.JoinRelation;
import org.sodeja.rel.relations.MinusRelation;
import org.sodeja.rel.relations.ProjectAwayRelation;
import org.sodeja.rel.relations.ProjectRelation;
import org.sodeja.rel.relations.RestrictRelation;
import org.sodeja.rel.relations.SummarizeRelation;

public class Domain {
	private final Set<BaseRelation> baseRelations = new HashSet<BaseRelation>();
	private final Map<String, Relation> relations = new HashMap<String, Relation>();
	private final Map<String, IntegrityCheck> checks = new HashMap<String, IntegrityCheck>();
	private final TransactionManager manager = new TransactionManager(this);
	
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
	
	private <T extends Relation> T remember(String name, T relation) {
		if(! StringUtils.isTrimmedEmpty(name)) {
			relations.put(name, relation);
			if(relation instanceof BaseRelation) {
				baseRelations.add((BaseRelation) relation);
			}
		}
		return relation;
	}
	
	public BaseRelation relation(String name, Attribute... attributes) {
		if(StringUtils.isTrimmedEmpty(name)) {
			throw new RuntimeException("Name is required for base relations");
		}
		return remember(name, new BaseRelation(this, name, attributes));
	}

	public Relation extend(String relation, CalculatedAttribute... attributes) {
		return extend(null, relation, attributes);
	}
	
	public Relation extend(String name, String relation, CalculatedAttribute... attributes) {
		return extend(name, resolve(relation), attributes);
	}

	public Relation extend(String name, Relation relation, CalculatedAttribute... attributes) {
		return remember(name, new ExtendRelation(name, relation, attributes));
	}
	
	public Relation restrict(String relation, Condition... conditions) {
		return restrict(null, relation, conditions);
	}

	public Relation restrict(String name, String relation, Condition... conditions) {
		return restrict(name, resolve(relation), conditions);
	}

	public Relation restrict(String name, Relation relation, Condition... conditions) {
		return remember(name, new RestrictRelation(name, relation, new HashSet<Condition>(Arrays.asList(conditions))));
	}
	
	public Relation project_away(Relation relation, String... attributes) {
		return project_away(null, relation, attributes);
	}
	
	public Relation project_away(String name, Relation relation, String... attributes) {
		return remember(name, new ProjectAwayRelation(name, relation, attributes));
	}

	public Relation project(String relation, String... attributes) {
		return project(resolve(relation), attributes);
	}
	
	public Relation project(Relation relation, String... attributes) {
		return project(null, relation, attributes);
	}
	
	public Relation project(String name, Relation relation, String... attributes) {
		return remember(name, new ProjectRelation(name, relation, attributes));
	}
	
	public Relation join(String relation, String other) {
		return join(relation, resolve(other));
	}

	public Relation join(String relation, Relation other) {
		return join(null, relation, other);
	}

	public Relation join(String name, String relation, Relation other) {
		return join(name, resolve(relation), other);
	}
	
	public Relation join(String name, Relation relation, Relation other) {
		return remember(name, new JoinRelation(name, relation, other));
	}
	
	public Relation minus(String name, Relation relation, String other) {
		return minus(name, relation, resolve(other));
	}

	public Relation minus(Relation relation, Relation other) {
		return minus(null, relation, other);
	}
	
	public Relation minus(String name, Relation relation, Relation other) {
		return remember(name, new MinusRelation(name, relation, other));
	}
	
	public Relation summarize(String relation, Relation other, Aggregate aggregate) {
		return summarize(resolve(relation), other, aggregate);
	}
	
	public Relation summarize(Relation relation, Relation other, Aggregate aggregate) {
		return summarize(null, relation, other, aggregate);
	}
	
	public Relation summarize(String name, String relation, Relation other, Aggregate aggregate) {
		return summarize(name, resolve(relation), other, aggregate);
	}
	
	public Relation summarize(String name, Relation relation, Relation other, Aggregate aggregate) {
		return remember(name, new SummarizeRelation(name, relation, other, aggregate));
	}

	public TransactionManager getTransactionManager() {
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
