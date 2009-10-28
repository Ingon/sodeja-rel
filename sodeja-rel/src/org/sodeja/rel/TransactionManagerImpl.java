package org.sodeja.rel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.sodeja.lang.IDGenerator;
import org.sodeja.rel.BaseRelationInfo.TransactionLogItem;

class TransactionManagerImpl implements TransactionManager {
	private final Domain domain;
	private final AtomicReference<Version> versionRef;
	private final ThreadLocal<TransactionInfo> state = new ThreadLocal<TransactionInfo>();
	private final ConcurrentLinkedQueue<TransactionInfo> order = new ConcurrentLinkedQueue<TransactionInfo>();
	private final IDGenerator idGen = new IDGenerator();
	
	protected TransactionManagerImpl(Domain domain) {
		this.domain = domain;
		versionRef = new AtomicReference<Version>(new Version(idGen.next(), new HashMap<BaseRelation, BaseRelationInfo>(), null));
	}
	
	public void begin() {
		TransactionInfo newInfo = null;
		while(true) {
			Version version = versionRef.get();
			newInfo = new TransactionInfo(version);
			version.transactionInfoCount.incrementAndGet();
			if(versionRef.compareAndSet(version, version)) {
				break;
			}
			version.transactionInfoCount.decrementAndGet();
		}
		state.set(newInfo);
		order.offer(newInfo);
		return;
	}
	
	private AtomicBoolean comitting = new AtomicBoolean();
	
	public void commit() {
		TransactionInfo info = state.get();
		if(info == null) {
			throw new TransactionRequiredException("No transaction");
		}
		if(info.rolledback) {
			throw new RollbackException("Already rolledback");
		}
		if(! info.hasChanges()) { // Only reads
			clearInfo(info);
			return;
		}
		
		waitOtherTransactions(info);
		try {
			commitTransaction(info);
		} finally {
			clearInfo(info);
		}
	}

	private void commitTransaction(TransactionInfo info) {
		try {
			domain.integrityCheck();
		} catch(ConstraintViolationException exc) {
			rollback();
			throw exc;
		}
		
		long verId = idGen.next();
		boolean result = versionRef.compareAndSet(info.version, new Version(verId, info.relationInfo, info.version));
		if(! result) {
			Version ver = versionRef.get();
			if(containsConflictingChanges(ver, info)) { 
				rollback();
				throw new RollbackException("");
			}
			
			Map<BaseRelation, BaseRelationInfo> relationInfo = merge(ver, info); // TODO here after merge we should again perform integrity checks!
			try {
				state.set(new TransactionInfo(info.version, relationInfo));
				domain.integrityCheck();
			} catch(ConstraintViolationException exc) {
				state.set(info);
				rollback();
				throw exc;
			}
			state.set(info);
			
			Version newVersion = new Version(verId, relationInfo, ver);
			result = versionRef.compareAndSet(ver, newVersion);
			if(! result) {
				throw new RuntimeException("WTF");
			}
			newVersion.clearOld();
		}
	}

	private void waitOtherTransactions(TransactionInfo info) {
		while(order.peek() != info) { // if we are not the head, wait for it
			synchronized(info) { 
				try {
					info.wait(100);
				} catch (InterruptedException e) {
					e.printStackTrace(); // Hmmm ? rollback?
				} 
			}
		}
	}

	private void clearInfo(TransactionInfo info) {
		order.poll();
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
	
	private boolean containsConflictingChanges(Version ver, TransactionInfo info) {
		Version curr = ver;
		while(curr != info.version) {
			if(checkDiff(curr.relationInfo, info.relationInfo)) {
				return true;
			}
			curr = curr.previousRef.get();
		}
		return false;
	}

	private boolean checkDiff(Map<BaseRelation, BaseRelationInfo> target, Map<BaseRelation, BaseRelationInfo> current) {
		for(Map.Entry<BaseRelation, BaseRelationInfo> c : current.entrySet()) { // TODO
			List<TransactionLogItem> tdelta = target.get(c.getKey()).txLog;
			if(tdelta == null) {
				continue;
			}
			
			List<TransactionLogItem> cdelta = c.getValue().txLog;
			for(TransactionLogItem id : cdelta) {
				if(tdelta.contains(id)) {
					return true;
				}
			}
		}
		return false;
	}
	
	private Map<BaseRelation, BaseRelationInfo> merge(Version ver, TransactionInfo info) {
		if(ver == info.version) {
			return info.relationInfo;
		}
		
		Map<BaseRelation, BaseRelationInfo> oldMergedInfo = merge(ver.previousRef.get(), info);
		Map<BaseRelation, BaseRelationInfo> mergedInfo = new HashMap<BaseRelation, BaseRelationInfo>(oldMergedInfo);
		for(BaseRelation r : ver.relationInfo.keySet()) {
			BaseRelationInfo versionInfo = ver.relationInfo.get(r);
			BaseRelationInfo threadInfo = oldMergedInfo.get(r);
			
			mergedInfo.put(r, threadInfo.merge(r, versionInfo));
		}
		
		return mergedInfo;
	}

	public void rollback() {
		TransactionInfo info = state.get();
		info.rolledback = true;
		clearInfo(info);
	}
	
	protected BaseRelationInfo get(BaseRelation key) {
		ValuesDelta info = getInfo(false);
		return info.relationInfo.get(key);
	}

	protected void set(BaseRelation key, BaseRelationInfo newInfo) {
		ValuesDelta info = getInfo(true);
		info.relationInfo.put(key, newInfo);
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
	
	private static abstract class ValuesDelta {
		protected final Map<BaseRelation, BaseRelationInfo> relationInfo;

		public ValuesDelta(Map<BaseRelation, BaseRelationInfo> relationInfo) {
			this.relationInfo = relationInfo;
		}
	}
	
	private static class TransactionInfo extends ValuesDelta {
		protected final Version version;
		protected boolean rolledback;
		
		public TransactionInfo(Version version) {
			super(version.newRelationInfo());
			this.version = version;
		}
		
		public TransactionInfo(Version version, Map<BaseRelation, BaseRelationInfo> relationInfo) {
			super(relationInfo);
			this.version = version;
		}
		
		protected boolean hasChanges() {
			if(! (relationInfo.keySet().equals(version.relationInfo.keySet()))) {
				return true;
			}
			
			for(BaseRelationInfo info : relationInfo.values()) {
				if(info.hasChanges()) {
					return true;
				}
			}
			return false;
		}
	}
	
	private static class Version extends ValuesDelta {
		protected final long id;
		protected final AtomicReference<Version> previousRef;
		protected final AtomicInteger transactionInfoCount = new AtomicInteger();
		
		public Version(long id, Map<BaseRelation, BaseRelationInfo> relationInfo, Version previous) {
			super(relationInfo);
			this.id = id;
			this.previousRef = new AtomicReference<Version>(previous);
		}
		
		public void clearOld() {
			Version previous = previousRef.get();
			if(previous != null) {
				previous.internalClear(this);
			}
		}

		private void internalClear(Version next) {
			Version previous = previousRef.get();
			if(previous != null) {
				previous.internalClear(this);
			}
			
			previous = previousRef.get();
			if(previous != null) {
				return;
			}
			
			if(transactionInfoCount.get() == 0) {
				next.previousRef.set(null);
			}
		}

		public Map<BaseRelation, BaseRelationInfo> newRelationInfo() {
			Map<BaseRelation, BaseRelationInfo> map = new HashMap<BaseRelation, BaseRelationInfo>();
			for(Map.Entry<BaseRelation, BaseRelationInfo> e : relationInfo.entrySet()) {
				BaseRelation rel = e.getKey();
				map.put(rel, rel.copyInfo(e.getValue()));
			}
			return map;
		}

		@Override
		public String toString() {
			return "V" + id;
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
