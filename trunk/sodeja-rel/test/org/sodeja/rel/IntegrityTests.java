package org.sodeja.rel;

public class IntegrityTests {
	public static void main(String[] args) {
		pkCheck(createDomain());
	}
	
	private static void pkCheck(Domain domain) {
		TransactionManager tx = domain.getTransactionManager();
		
		tx.begin();
		domain.insertPlain("Department", 
				"department_id", 0, 
				"name", "D0", 
				"manager", "U0");
		tx.commit();
		
		tx.begin();
		domain.insertPlain("Department", 
				"department_id", 1, 
				"name", "D1", 
				"manager", "U1");
		tx.commit();

		tx.begin();
		domain.insertPlain("Department", 
				"department_id", 1, 
				"name", "D1_", 
				"manager", "U1");
		tx.commit();
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
