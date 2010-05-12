package ru.circumflex.orm

import ORM._

// ## Association

class Association[R <: AnyRef, F <: AnyRef](val relation: Relation[R],
                                            name: String,
                                            uuid: String,
                                            val foreignRelation: Relation[F]
) extends ValueHolder[F](name, uuid) { assoc =>

  protected var _initialized: Boolean = false

  // ### Cascading actions for DDL

  protected var _onDelete: ForeignKeyAction = NO_ACTION
  protected var _onUpdate: ForeignKeyAction = NO_ACTION

  val field = new InternalField

  relation.addAssociation(this)

  // ### Commons

//  override def getValue(): F = super.getValue() match {
//    case null if (!_initialized && field() != None) =>
//      _initialized = true
//      // try to get from record cache
//      val id = field.get
//      _value = tx.getCachedRecord(foreignRelation, id) match {
//        case Some(record) => record
//        case None =>    // try to fetch lazily
//          val r = foreignRelation.as("root")
//          (SELECT (r.*) FROM (r) WHERE (r.id EQ id))
//              .unique
//              .getOrElse(null.asInstanceOf[F])
//      }
//      return _value
//    case value => return value
//  }
//
//  override def setValue(newValue: F): this.type = if (newValue == null) {
//    field.setValue(null.asInstanceOf[Long])
//    assoc._initialized = false
//    super.setValue(null.asInstanceOf[F])
//  } else newValue.id() match {
//    case None => throw new ORMException("Cannot assign transient record to association.")
//    case Some(id: Long) =>
//      field.setValue(id)
//      _initialized = true
//      super.setValue(newValue)
//  }

  def onDelete = _onDelete
  def onUpdate = _onUpdate

  def onDelete(action: ForeignKeyAction): this.type = {
    _onDelete = action
    return this
  }
  def ON_DELETE(action: ForeignKeyAction): this.type = onDelete(action)

  def onUpdate(action: ForeignKeyAction): this.type = {
    _onUpdate = action
    return this
  }
  def ON_UPDATE(action: ForeignKeyAction): this.type = onUpdate(action)

  class InternalField extends Field[Long](relation, name, uuid, dialect.longType) {

    override def getValue(from: AnyRef): Long = {
      try {
        recField match {
          case Some(x) =>
            x.getter.invoke(from).asInstanceOf[F] match {
              case null => -1
              case record => foreignRelation.idOf(record).getOrElse(-1)
            }
          case None => -1
        }
      } catch {case e: Exception => throw new RuntimeException(e)}
    }

    override def setValue(to: AnyRef, id: Long) {
      foreignRelation.recordOf(id) match {
        case Some(record) =>
          try {
            recField match {
              case Some(x) => x.setter.invoke(to, record)
              case None =>
            }
          } catch {case e: Exception => throw new RuntimeException(e)}
        case None =>
      }
    }

//    override def setValue(newValue: Long): this.type = {
//      super.setValue(newValue)
//      assoc._value = null.asInstanceOf[F]
//      assoc._initialized = false
//      return this
//    }
  }
}

// ## Inverse Associations

class InverseAssociation[P <: AnyRef, C <: AnyRef](val association: Association[C, P]) {
  
  def apply(record: P): Seq[C] = {
    if (association.foreignRelation.transient_?(record)) Nil
    else tx.getCachedInverse(record, this) match {  // lookup in cache
      case null => // lazy fetch
        val root = association.relation as "root"
        lastAlias(root.alias)
        val v = (SELECT (root.*) FROM (root) WHERE (association.field EQ association.foreignRelation.idOf(record).get)).list
        tx.updateInverseCache(record, this, v)
        v
      case l: Seq[C] => l
    }
  }

}
