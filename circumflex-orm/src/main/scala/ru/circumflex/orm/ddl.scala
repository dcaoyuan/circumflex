package ru.circumflex.orm


// ## DDL stuff

/**
 * A Unit-of-Work for generating database schema.
 */
class DDLUnit {
  import DDLUnit._

  private val log = ORM.getLogger(this)

  // ### Objects

  protected var _schemata: Seq[Schema] = Nil
  protected var _tables: Seq[Table[_]] = Nil
  protected var _views: Seq[View[_]] = Nil
  protected var _constraints: Seq[Constraint[_]] = Nil
  protected var _indexes: Seq[Index[_]] = Nil
  protected var _preAuxes: Seq[SchemaObject] = Nil
  protected var _postAuxes: Seq[SchemaObject] = Nil

  def schemata = _schemata
  def tables = _tables
  def views = _views
  def constraints = _constraints
  def indexes = _indexes
  def preAuxes = _preAuxes
  def postAuxes = _postAuxes

  protected var _msgs: Seq[Msg] = Nil
  def messages = _msgs
  def msgsArray: Array[Msg] = messages.toArray

  def this(objects: SchemaObject*) = {
    this()
    add(objects: _*)
  }

  def resetMsgs(): this.type = {
    _msgs = Nil
    this
  }

  def clear() = {
    _schemata = Nil
    _tables = Nil
    _views = Nil
    _constraints = Nil
    _preAuxes = Nil
    _postAuxes = Nil
    resetMsgs()
  }

  def add(objects: SchemaObject*): this.type = {
    objects foreach addObject
    this
  }

  def addObject(obj: SchemaObject): this.type = {
    def processRelation(r: Relation[_]) {
      addObject(r.schema)
      _preAuxes ++= (r.preAuxes filter (!_preAuxes.contains(_)))
      r.postAuxes foreach addObject
    }

    obj match {
      case t: Table[_] =>
        if (!_tables.contains(t)) {
          _tables :+= t
          t.constraints foreach addObject
          t.indexes foreach addObject
          processRelation(t)
        }
      case v: View[_] =>
        if (!_views.contains(v)) {
          _views :+= v
          processRelation(v)
        }
      case c: Constraint[_] =>
        if (!_constraints.contains(c))
          _constraints :+= c
      case s: Schema =>
        if (!_schemata.contains(s))
          _schemata :+= s
      case o =>
        if (!_postAuxes.contains(o))
          _postAuxes :+= o
    }
    this
  }

  // ### Workers

  protected def dropObjects(objects: Seq[SchemaObject]) =
    for (o <- objects.reverse) {
      if (o.isInstanceOf[Relation[_]]) o.asInstanceOf[Relation[_]].invalideCaches

      val sql = o.sqlDrop
      log.debug(sql)
      
      tx.execute{conn =>
        val st = conn.prepareStatement(sql)
        st.executeUpdate
        
        st.close
        _msgs :+= InfoMsg("DROP "  + o.objectName + ": OK", sql)
      } {ex =>
        _msgs :+= ErrorMsg("DROP " + o.objectName + ": " + ex.getMessage, sql)
      }
    }

  protected def createObjects(objects: Seq[SchemaObject]) =
    for (o <- objects) {
      val sql = o.sqlCreate
      log.debug(sql)

      tx.execute{conn =>
        val st = conn.prepareStatement(sql)
        st.executeUpdate
        
        st.close
        _msgs :+= InfoMsg("CREATE " + o.objectName + ": OK", sql)
      } {ex =>
        _msgs :+= ErrorMsg("CREATE " + o.objectName + ": " + ex.getMessage, sql)
      }
    }

  def setReferentialIntegrity(enable: Boolean) {
    val sql = ORM.dialect.setReferentialIntegrity(enable)

    tx.execute{conn =>
      val st = conn.prepareStatement(sql)
      st.executeUpdate
      
      st.close
      _msgs :+= InfoMsg("OK: ", sql)
    } {ex =>
      _msgs :+= ErrorMsg(ex.getMessage, sql)
    }
  }
  

  /**
   * Execute a DROP script for added objects.
   */
  def drop(): this.type = {
    resetMsgs()
    _drop()
  }
  def _drop(): this.type = {
    tx.execute{conn => 
      // We will commit every successfull statement.
      val autoCommit = conn.getAutoCommit
      conn.setAutoCommit(true)
      
      // Execute a script.
      dropObjects(postAuxes)
      dropObjects(views)
      if (ORM.dialect.supportsDropConstraints_?) dropObjects(constraints)
      dropObjects(tables)
      dropObjects(preAuxes)
      if (ORM.dialect.supportsSchema_?) dropObjects(schemata)
      
      // Restore auto-commit.
      conn.setAutoCommit(autoCommit)
      conn.close
      return this
    } {ex => throw ex}
  }

  /**
   * Execute a CREATE script for added objects.
   */
  def create(): this.type = {
    resetMsgs()
    _create()
  }
  def _create(): this.type = {
    tx.execute{conn =>
      // We will commit every successfull statement.
      val autoCommit = conn.getAutoCommit
      conn.setAutoCommit(true)
      
      // Execute a script.
      if (ORM.dialect.supportsSchema_?) createObjects(schemata)
      createObjects(preAuxes)
      createObjects(tables)
      createObjects(constraints)
      createObjects(views)
      createObjects(postAuxes)
      // disable Referential integrity check, @todo, it's better to not add RI constraints?
      setReferentialIntegrity(false)
     
      // Restore auto-commit.
      conn.setAutoCommit(autoCommit)
      conn.close
      return this
    } {ex => throw ex}
  }

  /**
   * Execute a DROP script and then a CREATE script.
   */
  def dropCreate(): this.type = {
    resetMsgs()
    _drop()
    _create()
  }

  override def toString: String = {
    var result = "Circumflex DDL Unit: "
    if (messages.size == 0) {
      val objectsCount = (schemata.size +
                          tables.size +
                          constraints.size +
                          views.size +
                          preAuxes.size +
                          postAuxes.size)
      result += objectsCount + " objects in queue."
    } else {
      val errorsCount = messages.filter(m => m.isInstanceOf[DDLUnit.ErrorMsg]).size
      val infoCount = messages.filter(m => m.isInstanceOf[DDLUnit.InfoMsg]).size
      result += infoCount + " successful statements, " + errorsCount + " errors."
    }
    result
  }
}

// ### Messages

object DDLUnit {
  trait Msg {
    def body: String
    def sql: String
  }
  final case class InfoMsg(val body: String, val sql: String) extends Msg
  final case class ErrorMsg(val body: String, val sql: String) extends Msg
}