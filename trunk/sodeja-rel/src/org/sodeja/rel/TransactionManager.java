package org.sodeja.rel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.sodeja.collections.PersistentSet;

class TransactionManager {
	private final Domain domain;
	private final AtomicReference<Version> versionRef;
	private final ThreadLocal<TransactionInfo> state = new ThreadLocal<TransactionInfo>();
	private final ConcurrentLinkedQueue<TransactionInfo> order = new ConcurrentLinkedQueue<TransactionInfo>();
	private final UUIDGenerator idGen = new UUIDGenerator();
	
	protected TransactionManager(Domain domain) {
		this.domain = domain;
		versionRef = new AtomicReference<Version>(new Version(idGen.next(), new HashMap<BaseRelation, PersistentSet<BaseEntity>>(), new HashMap<BaseRelation, PersistentSet<UUID>>(), null));
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
		clearOldVersions();
	}

	private void clearInfo(TransactionInfo info) {
		order.remove(info);
		state.remove();
		info.version.transactionInfoCount.decrementAndGet();
		
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
	
	protected void clearOldVersions() {
		Version curr = versionRef.get();
		while(curr.previousRef.get() != null) {
			Version prev = curr.previousRef.get();
			if(prev.transactionInfoCount.get() == 0) {
				curr.previousRef.set(prev.previousRef.get());
			} else {
				curr = curr.previousRef.get();
			}
		}
	}
	
	private boolean isTouched(Version ver, TransactionInfo info) { // Poor naming - idea is to check delta on all versions till our version for modifications of same entities
		Version curr = ver;
		while(curr != info.version) {
			if(checkDiff(curr.delta, info.delta)) {
				return true;
			}
			curr = curr.previousRef.get();
		}
		return false;
	}

	private boolean checkDiff(Map<BaseRelation, PersistentSet<UUID>> target, Map<BaseRelation, PersistentSet<UUID>> current) {
		for(Map.Entry<BaseRelation, PersistentSet<UUID>> c : current.entrySet()) {
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
	
	protected PersistentSet<BaseEntity> get(BaseRelation key) {
		ValuesDelta info = getInfo(false);
		return info.values.get(key);
	}

	protected void set(BaseRelation key, PersistentSet<BaseEntity> value, PersistentSet<UUID> delta) {
		ValuesDelta info = getInfo(true);
		info.values.put(key, value);
		
		PersistentSet<UUID> prevDelta = info.delta.get(key);
		if(prevDelta != null) {
			prevDelta = prevDelta.addAllValues(delta);
		} else {
			prevDelta = new PersistentSet<UUID>();
		}
		info.delta.put(key, prevDelta);
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
		protected final Map<BaseRelation, PersistentSet<BaseEntity>> values;
		protected final Map<BaseRelation, PersistentSet<UUID>> delta;
		
		public ValuesDelta(Map<BaseRelation, PersistentSet<BaseEntity>> values, Map<BaseRelation, PersistentSet<UUID>> delta) {
			this.values = values;
			this.delta = delta;
		}
	}
	
	private class TransactionInfo extends ValuesDelta {
		protected final Version version;
		protected boolean rolledback;
		
		public TransactionInfo(Version version) {
			super(new HashMap<BaseRelation, PersistentSet<BaseEntity>>(version.values), new HashMap<BaseRelation, PersistentSet<UUID>>());
			this.version = version;
			this.version.transactionInfoCount.incrementAndGet();
		}
	}
	
	private class Version extends ValuesDelta {
		protected final UUID id;
		protected final AtomicReference<Version> previousRef;
		protected final AtomicInteger transactionInfoCount = new AtomicInteger();
		
		public Version(UUID id, Map<BaseRelation, PersistentSet<BaseEntity>> values, Map<BaseRelation, PersistentSet<UUID>> delta, Version previous) {
			super(values, delta);
			this.id = id;
			this.previousRef = new AtomicReference<Version>(previous);
		}
	}
	
	protected int countVersions() {
		Version curr = versionRef.get();
		int i = 0;
		while(curr != null) {
			i++;
			curr = curr.previousRef.get();
		}
		return i;
	}
}
