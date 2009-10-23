package org.sodeja.rel;

public class PerformanceTest {
	public static void main(String[] args) {
		Domain domain = IntegrityTests.createDomain();
		Relation dep = domain.resolveBase("Department");
		int depId = 0;
		int number = 1;
		long totalStart = System.currentTimeMillis();
		while(dep.select().size() <= 10000) {
			long start = System.currentTimeMillis();
			domain.txm().begin();
			int idStart = depId;
			int idEnd = insert(domain, depId, 16);
			depId = idEnd;
			
			domain.txm().commit();
			long end = System.currentTimeMillis();
			long time = end - start;
			System.out.println("Insert Time: " + time);
			
			
			
			start = System.currentTimeMillis();
			domain.txm().begin();
			
			update(domain, idStart, 8);
			
			domain.txm().commit();
			end = System.currentTimeMillis();
			time = end - start;
			System.out.println("Update Time: " + time);

			
			
			start = System.currentTimeMillis();
			domain.txm().begin();
			
			delete(domain, idStart + 8, 8);
			
			domain.txm().commit();
			end = System.currentTimeMillis();
			time = end - start;
			System.out.println("Delete Time: " + time);
//			if(time > 200) {
//				System.out.println("T: " + dep.select().size());
//				System.out.println("V: " + domain.getTransactionManager().countVersions());
//			}
		}
		long totalEnd = System.currentTimeMillis();
		System.out.println("End: " + (totalEnd - totalStart));
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

	private static void update(Domain domain, int baseId, int size) {
		for(int i = 0;i < size; i++) {
			domain.updatePlain("Department", Conditions.likeValues("department_id", baseId + i), 
					"name", "fairlyLongName1", 
					"manager", "fairlyLongManagerName1");
		}
	}

	private static void delete(Domain domain, int baseId, int size) {
		for(int i = 0;i < size; i++) {
			domain.deletePlain("Department", "department_id", baseId + i); 
		}
	}
}
