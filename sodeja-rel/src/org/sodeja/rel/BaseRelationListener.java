package org.sodeja.rel;

public interface BaseRelationListener {
	public void inserted(BaseRelation relation, BaseEntity entity);
	public void updated(BaseRelation relation, BaseEntity oldEntity, BaseEntity newEntity);
	public void deleted(BaseRelation relation, BaseEntity entity);
}
