package org.sodeja.rel;

import java.util.Date;

public enum Types implements Type {
	STRING(String.class),
	INT(int.class, Integer.class),
	DOUBLE(double.class, Double.class),
	DATE(Date.class), 
	BOOL(boolean.class, Boolean.class);
	
	private final Class<?>[] supported;
	
	private Types(Class<?>... supported) {
		this.supported = supported;
	}

	@Override
	public boolean accepts(Object o) {
		if(o == null) {
			return true;
		}
		Class<?> oc = o.getClass();
		for(Class<?> c : supported) {
			if(c.isAssignableFrom(oc)) {
				return true;
			}
		}
		return false;
	}
}
