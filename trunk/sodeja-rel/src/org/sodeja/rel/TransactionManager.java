package org.sodeja.rel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

class TransactionManager {
	private AtomicReference<TransactionInfo> currentInfo = new AtomicReference<TransactionInfo>();
	private ThreadLocal<TransactionInfo> state = new ThreadLocal<TransactionInfo>();
	
	protected TransactionManager() {
		currentInfo.set(new TransactionInfo(null, 0, new HashMap<BaseRelation, Set<Entity>>()));
	}
	
	public void begin() {
		state.set(new TransactionInfo(currentInfo.get()));
	}
	
	public void commit() {
		TransactionInfo info = state.get();
		currentInfo.compareAndSet(info.parent, info);
		state.remove();
	}
	
	public void rollback() {
		state.remove();
	}
	
	protected Set<Entity> get(BaseRelation key) {
		Map<BaseRelation, Set<Entity>> currentValues = getMap();
		return currentValues.get(key);
	}

	protected void set(BaseRelation key, Set<Entity> value) {
		Map<BaseRelation, Set<Entity>> currentValues = getMap();
		currentValues.put(key, value);
	}

	private Map<BaseRelation, Set<Entity>> getMap() {
		TransactionInfo transactionInfo = state.get();
		if(transactionInfo == null) {
			return currentInfo.get().currentValues;
		}
		Map<BaseRelation, Set<Entity>> currentValues = transactionInfo.currentValues;
		if(currentValues == null) {
			currentValues = currentInfo.get().currentValues;
		}
		return currentValues;
	}
	
	private class TransactionInfo {
		protected final TransactionInfo parent;
		protected final long lastCommitDate;
		protected final Map<BaseRelation, Set<Entity>> currentValues;
		
		public TransactionInfo(TransactionInfo other) {
			this(other, other.lastCommitDate, new HashMap<BaseRelation, Set<Entity>>(other.currentValues));
		}
		
		public TransactionInfo(TransactionInfo other, long commitDate, Map<BaseRelation, Set<Entity>> currentValues) {
			this.parent = other;
			this.lastCommitDate = commitDate;
			this.currentValues = currentValues;
		}
	}
}
