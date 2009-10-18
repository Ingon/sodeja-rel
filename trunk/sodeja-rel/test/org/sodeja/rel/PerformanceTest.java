package org.sodeja.rel;

public class PerformanceTest {
	public static void main(String[] args) {
		Domain domain = IntegrityTests.createDomain();
		Relation dep = domain.resolveBase("Department");
		int depId = 0;
		int number = 1;
		while(dep.select().size() <= 100000) {
			long start = System.currentTimeMillis();
			domain.getTransactionManager().begin();
			int idStart = depId;
			int idEnd = insert(domain, depId, 16);
			depId = idEnd;
			
			domain.getTransactionManager().commit();
			long end = System.currentTimeMillis();
			long time = end - start;
			System.out.println("N: " + number++ + " Time: " + time);
			if(time > 200) {
				System.out.println("T: " + dep.select().size());
				System.out.println("V: " + domain.getTransactionManager().countVersions());
			}
		}
		System.out.println("End");
	}

	private static int insert(Domain domain, int baseId, int size) {
		for(int i = 0;i < size; i++) {
			domain.insertPlain("Department", 
					"department_id", baseId + i, 
					"name", "fairlyLongName", 
					"manager", "fairlyLongManagerName");
		}
		return baseId + size;
	}
}
