package org.sodeja.rel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

class TransactionManager {
	private final Domain domain;
	private final AtomicReference<Version> versionRef;
	private final ThreadLocal<TransactionInfo> state = new ThreadLocal<TransactionInfo>();
	private final ConcurrentLinkedQueue<TransactionInfo> order = new ConcurrentLinkedQueue<TransactionInfo>();
	private final UUIDGenerator idGen = new UUIDGenerator();
	
	protected TransactionManager(Domain domain) {
		this.domain = domain;
		versionRef = new AtomicReference<Version>(new Version(idGen.next(), new HashMap<BaseRelation, Set<BaseEntity>>(), new HashMap<BaseRelation, Set<UUID>>(), null));
	}
	
	public void begin() {
		state.set(new TransactionInfo(versionRef.get()));
		order.offer(state.get());
	}
	
	public void commit() {
		TransactionInfo info = state.get();
		if(info == null) {
			throw new TransactionRequiredException("No transaction");
		}
		if(info.rolledback) {
			throw new RollbackException("Already rolledback");
		}
		if(info.delta.size() == 0) { // Only reads
			clearInfo(info);
			return;
		}
		while(order.peek() != info) { // if we are not the head, wait for it
			synchronized(info) { 
				try {
					info.wait(100);
				} catch (InterruptedException e) {
					e.printStackTrace(); // Hmmm ? rollback?
				} 
			}
		}
		
		try {
			domain.integrityCheck();
		} catch(ConstraintViolationException exc) {
			rollback();
			throw exc;
		}
		
		UUID verId = idGen.next();
		boolean result = versionRef.compareAndSet(info.version, new Version(verId, info.values, info.delta, info.version));
		if(! result) {
			Version ver = versionRef.get();
			if(isTouched(ver, info)) {
				rollback();
				throw new RollbackException("");
			}
			result = versionRef.compareAndSet(ver, new Version(verId, info.values, info.delta, ver));
		}
		
		clearInfo(info);
	}

	private void clearInfo(TransactionInfo info) {
		order.remove(info);
		state.remove();
		
		TransactionInfo nextInfo = null;
		while((nextInfo = order.peek()) != null && nextInfo.rolledback) {
			order.poll();
		}
		if(nextInfo != null) {
			synchronized (nextInfo) {
				nextInfo.notifyAll();
			}
		}
	}
	
	private boolean isTouched(Version ver, TransactionInfo info) { // Poor naming - idea is to check delta on all versions till our version for modifications of same entities
		Version curr = ver;
		while(curr != info.version) {
			if(checkDiff(curr.delta, info.delta)) {
				return true;
			}
			curr = curr.previous;
		}
		return false;
	}

	private boolean checkDiff(Map<BaseRelation, Set<UUID>> target, Map<BaseRelation, Set<UUID>> current) {
		for(Map.Entry<BaseRelation, Set<UUID>> c : current.entrySet()) {
			Set<UUID> tdelta = target.get(c.getKey());
			if(tdelta == null) {
				continue;
			}
			
			Set<UUID> cdelta = c.getValue();
			for(UUID id : cdelta) {
				if(tdelta.contains(id)) {
					return true;
				}
			}
		}
		return false;
	}

	public void rollback() {
		TransactionInfo info = state.get();
		info.rolledback = true;
		clearInfo(info);
	}
	
	protected Set<BaseEntity> get(BaseRelation key) {
		ValuesDelta info = getInfo(false);
		return info.values.get(key);
	}

	protected void set(BaseRelation key, Set<BaseEntity> value, Set<UUID> delta) {
		ValuesDelta info = getInfo(true);
		info.values.put(key, value);
		
		Set<UUID> merged = new HashSet<UUID>(delta);
		Set<UUID> prevDelta = info.delta.get(key);
		if(prevDelta != null) {
			merged.addAll(prevDelta);
		}
		info.delta.put(key, delta);
	}

	private ValuesDelta getInfo(boolean withTransaction) {
		TransactionInfo transactionInfo = state.get();
		if(transactionInfo != null && transactionInfo.rolledback) {
			throw new RollbackException("Transaction already rolledback");
		}
		if(withTransaction && transactionInfo == null) {
			throw new TransactionRequiredException("Must be in transaction");
		}
		return transactionInfo != null ? transactionInfo : versionRef.get();
	}
	
	private abstract class ValuesDelta {
		protected final Map<BaseRelation, Set<BaseEntity>> values;
		protected final Map<BaseRelation, Set<UUID>> delta;
		
		public ValuesDelta(Map<BaseRelation, Set<BaseEntity>> values, Map<BaseRelation, Set<UUID>> delta) {
			this.values = values;
			this.delta = delta;
		}
	}
	
	private class TransactionInfo extends ValuesDelta {
		protected final Version version;
		protected boolean rolledback;
		
		public TransactionInfo(Version version) {
			super(new HashMap<BaseRelation, Set<BaseEntity>>(version.values), new HashMap<BaseRelation, Set<UUID>>());
			this.version = version;
		}
	}
	
	private class Version extends ValuesDelta {
		protected final UUID id;
		protected final Version previous;
		
		public Version(UUID id, Map<BaseRelation, Set<BaseEntity>> values, Map<BaseRelation, Set<UUID>> delta, Version previous) {
			super(values, delta);
			this.id = id;
			this.previous = previous;
		}
	}
}
