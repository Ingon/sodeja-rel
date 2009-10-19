package org.sodeja.rel;

public class UUID {
	private final long data;
	
	public UUID(long data) {
		this.data = data;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (data ^ (data >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(! (obj instanceof UUID)) {
			return false;
		}
		return data == ((UUID) obj).data;
	}

	@Override
	public String toString() {
		return "@" + data;
	}
}
