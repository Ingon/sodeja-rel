package org.sodeja.rel;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import org.sodeja.collections.SetUtils;
import org.sodeja.lang.Range;

public class SQLThreadedTest {
	private static Object GLOBAL_LOCK = new Object();
	
	private static class TestThread extends Thread {
		private final Connection conn;
		private final Range insertRange;
		private final Range insertAnDeleteRange;
		
		public TestThread(String name, Connection conn, Range insertRange, Range insertAnDeleteRange) {
			this.setName(name);
			this.conn = conn;
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
			
			try {
				PreparedStatement insert = conn.prepareStatement("insert into departments values (?, ?, ?)");
				PreparedStatement delete = conn.prepareStatement("delete from departments where id = ?");
				
				for(Integer i : insertRange) {
					insert.setInt(1, i);
					insert.setString(2, "fairlyLongName");
					insert.setString(3, "fairlyLongManagerName");
					insert.executeUpdate();
					
					conn.commit();
				}
	
				for(Integer i : insertAnDeleteRange) {
					insert.setInt(1, i);
					insert.setString(2, "fairlyLongName");
					insert.setString(3, "fairlyLongManagerName");
					insert.executeUpdate();
					
					conn.commit();
	
					delete.setInt(1, i);
	
					conn.commit();
				}
			} catch(SQLException e) {
				e.printStackTrace();
			}
			
			System.out.println("Finished");
		}
	}
	
	public static void main(String[] args) throws Exception {
//		System.setProperty("derby.system.home", "file://d:/temp");
		Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
		
		Connection ccon = DriverManager.getConnection("jdbc:derby:DepartmentTest;create=true");
		ccon.createStatement().execute("create table departments (id INTEGER NOT NULL PRIMARY KEY, name varchar(255), manager varchar(255))");
		
		TestThread[] threads = new TestThread[10];
		int sz = 100;
		int delBase = sz * threads.length;
		
		for(Integer i : Range.of(threads)) {
//			Connection connection = DriverManager.getConnection("jdbc:derby:DepartmentTest;user=dbuser;password=dbuserpwd");
			Connection connection = DriverManager.getConnection("jdbc:derby:DepartmentTest;");
			connection.setAutoCommit(false);
			threads[i] = new TestThread("TH" + i, connection, new Range(i * sz, (i + 1) * sz), new Range(delBase + i * sz, delBase + (i + 1) * sz));
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
		
//		System.out.println("COUNT: " + (domain.resolveBase("Department").select().size()));
		System.out.println("TIME: " + (end - begin));
		
//		BaseRelation rel = domain.resolveBase("Department");
//		Iterator<Entity> it = rel.select().iterator();
//		if(it.hasNext()) {
//			Entity e = it.next();
//			Attribute idatt = e.getAttributeValue("id").attribute;
//			for(Integer i : new Range(0, threads.length * sz)) {
//				if(rel.selectByKey(new Entity(SetUtils.asSet(new AttributeValue(idatt, i)))) == null) {
//					System.out.println("Missing: " + i);
//				}
//			}
//		}
	}
}
