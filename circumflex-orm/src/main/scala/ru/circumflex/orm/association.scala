package ru.circumflex.orm

// ## Association

class Association[R, F](val relation: Relation[R],
                        val name: String,
                        val foreignRelation: Relation[F],
                        prefetch_? : Boolean = false
) {

  val uuid = relation.uuid + "." + name

  val field: Field[R, Long] = new InternalField

  /*! Column definition methods delegate to underlying field. */
  def notNull_?(): Boolean = field.notNull_?
  def NOT_NULL(): this.type = {
    field.NOT_NULL
    this
  }
  def unique_?(): Boolean = field.unique_?
  def UNIQUE(): this.type = {
    field.UNIQUE
    this
  }
  def defaultExpression: Option[String] = field.defaultExpression
  def DEFAULT(expr: String): this.type = {
    field.DEFAULT(expr)
    this
  }



  protected var _initialized: Boolean = false

  // ### Cascading actions for DDL

  protected var _onDelete: ForeignKeyAction = NO_ACTION
  protected var _onUpdate: ForeignKeyAction = NO_ACTION

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

  def ON_DELETE(action: ForeignKeyAction): this.type = {
    _onDelete = action
    this
  }

  def ON_UPDATE(action: ForeignKeyAction): this.type = {
    _onUpdate = action
    this
  }

  class InternalField extends Field[R, Long](relation, name, ORM.dialect.longType, org.apache.avro.Schema.Type.LONG) {

    override def getValue(from: R): Long = {
      recField match {
        case Some(x) =>
          try {
            x.getter.invoke(from).asInstanceOf[F] match {
              case null => -1
              case record => foreignRelation.idOf(record).getOrElse(-1)
            }
          } catch {
            case e: Exception => throw new RuntimeException(e)
          }
        case None => -1
      }
    }

    override def setValue(to: R, id: Any): Option[() => Unit] = {
      if (id == -1) return None
      
      // return a lazy fetcher so the foreign record may has been ready after all query's read(rs) done
      val lazyFetcher = {() =>
        foreignRelation.recordOf(id.asInstanceOf[Long]) match {
          case Some(fRecord) => _setValue(to, fRecord) // set instance to's reference field to foreign record
          case None =>
        }
        ()
      }
      
      Some(lazyFetcher)
    }

  }

  def apply(record: R): Option[F] = {
    if (relation.transient_?(record)) None
    else {
      val id = relation.idOf(record)
      val root = foreignRelation
      (SELECT (root.*) FROM (relation JOIN root) WHERE (field EQ id)).unique map {fRecord =>
        //tx.updateRecordCache(foreignRelation, fRecord)
        field.asInstanceOf[InternalField]._setValue(record, fRecord)
        fRecord
      }
    }
  }
}

// ## Inverse Associations

class InverseAssociation[P, C](val association: Association[C, P]) {
  
  def apply(record: P): Seq[C] = {
    if (association.foreignRelation.transient_?(record)) Nil
    else tx.getCachedInverse(record, this) match {  // lookup in cache
      case null => // lazy fetch
        val id = association.foreignRelation.idOf(record)
        val root = association.relation
        val l = (SELECT (root.*) FROM (root) WHERE (association.field EQ id) list)
        //tx.updateInverseCache(record, this, l)
        l
      case l: Seq[C] => l
    }
  }

}
