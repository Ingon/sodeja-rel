package org.sodeja.rel;

import java.util.Iterator;

import org.sodeja.collections.SetUtils;
import org.sodeja.lang.Range;

public class ThreadedTest {
	private static Object GLOBAL_LOCK = new Object();
	
	private static class TestThread extends Thread {
		private final Domain domain;
		private final Range insertRange;
		private final Range insertAnDeleteRange;
		
		public TestThread(String name, Domain domain, Range insertRange, Range insertAnDeleteRange) {
			this.setName(name);
			this.domain = domain;
			this.insertRange = insertRange;
			this.insertAnDeleteRange = insertAnDeleteRange;
		}

		@Override
		public void run() {
			synchronized (GLOBAL_LOCK) {
				try {
					GLOBAL_LOCK.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			for(Integer i : insertRange) {
				domain.getTransactionManager().begin();
				
				domain.insertPlain("Department", 
						"id", i, 
						"name", "fairlyLongName", 
						"manager", "fairlyLongManagerName");
				
				domain.getTransactionManager().commit();
			}

			for(Integer i : insertAnDeleteRange) {
				domain.getTransactionManager().begin();

				domain.insertPlain("Department", 
						"id", i, 
						"name", "fairlyLongName", 
						"manager", "fairlyLongManagerName");
				
				domain.getTransactionManager().commit();

				domain.getTransactionManager().begin();

				domain.deletePlain("Department", "id", i); 

				domain.getTransactionManager().commit();
			}
			
			for(Integer i : insertRange) {
				domain.getTransactionManager().begin();
				
				domain.insertPlain("Employee", 
						"id", i, 
						"name", "fairlyLongName", 
						"department_id", i);
				
				domain.getTransactionManager().commit();
			}
			
			System.out.println("Finished");
		}
	}
	
	public static void main(String[] args) {
		Domain domain = IntegrityTests.createDomain();
		
		TestThread[] threads = new TestThread[10];
		int sz = 200;
		int delBase = sz * threads.length;
		
		for(Integer i : Range.of(threads)) {
			threads[i] = new TestThread("TH" + i, domain, new Range(i * sz, (i + 1) * sz), new Range(delBase + i * sz, delBase + (i + 1) * sz));
			threads[i].start();
		}
		
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		long begin = System.currentTimeMillis();
		synchronized (GLOBAL_LOCK) {
			GLOBAL_LOCK.notifyAll();
		}
		
		OUTER: while(true) {
			for(TestThread t : threads) {
				if(t.isAlive()) {
					continue OUTER;
				}
			}
			break;
		}
		long end = System.currentTimeMillis();
		
		System.out.println("COUNT: " + (domain.resolveBase("Department").select().size()));
		System.out.println("TIME: " + (end - begin));
		
		BaseRelation rel = domain.resolveBase("Department");
		Iterator<Entity> it = rel.select().iterator();
		if(it.hasNext()) {
			Entity e = it.next();
			Attribute idatt = e.getAttributeValue("id").attribute;
			for(Integer i : new Range(0, threads.length * sz)) {
				if(rel.selectByKey(new Entity(SetUtils.asSet(new AttributeValue(idatt, i)))) == null) {
					System.out.println("Missing: " + i);
				}
			}
		}
	}
}
