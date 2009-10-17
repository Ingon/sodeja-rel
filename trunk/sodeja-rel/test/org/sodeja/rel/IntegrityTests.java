package org.sodeja.rel;


public class IntegrityTests {
	private static boolean log = false;

	public static void main(String[] args) {
		pkCheck(createDomain());
		fkCheck(createDomain());
	}
	
	private static void pkCheck(Domain domain) {
		TransactionManager tx = domain.getTransactionManager();
		
		tx.begin();
		domain.insertPlain("Department", 
				"department_id", 0, 
				"name", "D0", 
				"manager", "U0");
		expectSucc(tx);
		
		tx.begin();
		domain.insertPlain("Department", 
				"department_id", 1, 
				"name", "D1", 
				"manager", "U1");
		expectSucc(tx);

		tx.begin();
		domain.insertPlain("Department", 
				"department_id", 1, 
				"name", "D1_", 
				"manager", "U1");
		expectFail(tx, "Primary");

		tx.begin();
		domain.insertPlain("Department", 
				"department_id", 2, 
				"name", "D2", 
				"manager", "U2");
		domain.insertPlain("Department", 
				"department_id", 2, 
				"name", "D2_", 
				"manager", "U2");
		expectFail(tx, "Primary");
	}
	
	private static void fkCheck(Domain domain) {
		TransactionManager tx = domain.getTransactionManager();
		
		tx.begin();
		domain.insertPlain("Department", 
				"department_id", 0, 
				"name", "D0", 
				"manager", "U0");
		expectSucc(tx);
		
		tx.begin();
		domain.insertPlain("Employee", 
				"employee_id", 0, 
				"name", "E0", 
				"department_id", 0);
		expectSucc(tx);

		tx.begin();
		domain.insertPlain("Employee", 
				"employee_id", 1, 
				"name", "E0", 
				"department_id", 1);
		expectFail(tx, "Foreign");

		tx.begin();
		domain.insertPlain("Employee", 
				"employee_id", 1, 
				"name", "E0", 
				"department_id", 1);
		domain.insertPlain("Department", 
				"department_id", 1, 
				"name", "D1", 
				"manager", "U1");
		expectSucc(tx);
	}

	private static void fail() {
		throw new RuntimeException("Test Failed");
	}
	
	private static void expectSucc(TransactionManager tx) {
		try {
			tx.commit();
		} catch(ConstraintViolationException exc) {
			if(log) {
				exc.printStackTrace();
			}
			fail();
		}
	}
	
	private static void expectFail(TransactionManager tx, String begin) {
		try {
			tx.commit();
			fail();
		} catch(ConstraintViolationException exc) {
			if(log) {
				exc.printStackTrace();
			}
			if(! exc.getMessage().startsWith(begin)) {
				fail();
			}
		}
	}
	
	private static Domain createDomain() {
		Domain domain = new Domain();
		domain.getTransactionManager().begin();
		
		BaseRelation empl = domain.relation("Employee", 
				new Attribute("employee_id", Types.INT),
				new Attribute("name", Types.STRING),
				new Attribute("department_id", Types.INT))
				.primaryKey("employee_id");
		
		BaseRelation dep = domain.relation("Department", 
				new Attribute("department_id", Types.INT),
				new Attribute("name", Types.STRING),
				new Attribute("manager", Types.STRING))
				.primaryKey("department_id");
		
		empl.foreignKey(dep, "department_id");
		
		domain.getTransactionManager().commit();
		return domain;
	}
}
