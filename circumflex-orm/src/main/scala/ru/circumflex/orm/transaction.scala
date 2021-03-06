package ru.circumflex.orm

import java.sql.{Connection, SQLException}
import collection.mutable.HashMap
import java.util.logging.Level
import java.util.logging.Logger

// ## Transaction management

// ### Transaction demarcation

// *Transaction demarcation* refers to setting the transaction boundaries.
//
// Datatabase transaction boundaries are always necessary. No communication with the
// database can occur outside of a database transaction (this seems to confuse many
// developers who are used to the auto-commit mode). Always use clear transaction
// boundaries, even for read-only operations. Depending on your isolation level and
// database capabilities this might not be required but there is no downside if you
// always demarcate transactions explicitly.
//
// There are several popular transaction demarcation patterns for various application types,
// most of which operate with some sort of "context" or "scope", to which a single
// transaction corresponds. For example, in web applications a transaction may correspond
// to a single request.

/**
 * ### TransactionManager interface
 *
 * *Transaction manager* aims to help developers demarcate their transactions
 * by providing contextual *current* transaction. By default it uses `ThreadLocal`
 * to bind contextual transactions (a separate transaction is allocated for each thread,
 * and each thread works with one transaction at a given time). You can
 * provide your own transaction manager by implementing the `TransactionManager`
 * trait and setting the `orm.transactionManager` configuration parameter.</p>
 *
 * Defines a contract to open stateful transactions and return
 * thread-locally current transaction.
 */
trait TransactionManager {
  private val log = Logger.getLogger(getClass.getName)
  
  private val threadLocalContext = new ThreadLocal[Transaction]

  /**
   * Does transaction manager has live current transaction?
   */
  def hasLiveTransaction_?(): Boolean =
    threadLocalContext.get != null && threadLocalContext.get.isLive

  /**
   * Retrieve a contextual transaction.
   */
  def getTransaction: Transaction = {
    if (!hasLiveTransaction_?) threadLocalContext.set(openTransaction)
    threadLocalContext.get
  }

  /**
   * Sets a contextual transaction to specified `tx`.
   */
  def setTransaction(tx: Transaction): Unit = threadLocalContext.set(tx)

  /**
   * Open new stateful transaction.
   */
  @throws(classOf[SQLException])
  def openTransaction(): Transaction = new Transaction()

  /**
   * Execute a block with actions in state-safe manner (does cleanup afterwards) without transaction.
   */
  @throws(classOf[SQLException])
  def executeOnce[A](action: Connection => A)(errAction: Throwable => A): A = {
    var conn: Connection = null
    try {
      conn = ORM.connectionProvider.openConnection
      action(conn)
    } catch {
      case e: Throwable => errAction(e)
    } finally {
      if (conn != null) {
        try {
          conn.close
        } catch {case _: Throwable =>}
      }
    }
  }

  /**
   * A shortcut for `getTransaction.execute(actions)`.
   */
  @throws(classOf[SQLException])
  def execute[A](action: Connection => A)(errAction: Throwable => A) =
    getTransaction.execute(action)(errAction)

  /**
   * Execute specified `block` in specified `transaction` context and
   * commits the `transaction` afterwards.
   *
   * If any exception occur, rollback the transaction and rethrow an
   * exception.
   *
   * The contextual transaction is replaced with specified `transaction` and
   * is restored after the execution of `block`.
   */
  @throws(classOf[SQLException])
  def executeInContext(transaction: Transaction)(block: => Unit) = {
    val prevTx: Transaction = if (hasLiveTransaction_?) getTransaction else null
    try {
      setTransaction(transaction)
      block
      if (transaction.isLive) {
        transaction.commit
        log.fine("Committed current transaction.")
      }
    } catch {
      case e: Throwable =>
        if (transaction.isLive) {
          transaction.rollback
          log.warning("Rolled back current transaction.")
        }
        throw e
    } finally if (transaction.isLive) {
      transaction.close
      log.fine("Closed current connection.")
      setTransaction(prevTx)
    }
  }
}

object DefaultTransactionManager extends TransactionManager

// ### Stateful Transactions

/**
 * The point to use extra-layer above standard JDBC connections is to maintain
 * a cache for each transaction.
 */
@throws(classOf[SQLException])
class Transaction {
  private val log = Logger.getLogger(this.getClass.getName)
  
  /**
   * Undelying JDBC connection.
   */
  private var _connection: Connection = _
  private def connection = {
    if (_connection == null) {
      try {
        _connection = ORM.connectionProvider.openConnection
      } catch {
        case ex: Throwable => log.log(Level.SEVERE, ex.getMessage, ex); throw ex
      }
    }
    _connection
  }
  
  /**
   * Is underlying connection alive?
   * @Note: According to javadoc: connection.isClosed generally cannot be called to determine whether 
   * a connection to a database is valid or invalid. A typical client can determine that a connection 
   * is invalid by catching any exceptions that might be thrown when an operation is attempted.
   */
  def isLive(): Boolean = _connection != null && !_connection.isClosed
  
  /**
   * Execute an attached block within the transaction scope.
   */
  @throws(classOf[SQLException])
  def execute[A](connAction: Connection => A)(errAction: Throwable => A): A = {
    try {
      connAction(connection)
    } catch {
      case e: Throwable => errAction(e)
    }
  }

  /**
   * Commit the transaction (and close underlying connection if `autoClose` is set to `true`).
   */
  @throws(classOf[SQLException])
  def commit() {
    if (isLive && !_connection.getAutoCommit) _connection.commit
    close()
  }

  /**
   * Rollback the transaction (and close underlying connection if `autoClose` is set to `true`).
   */
  @throws(classOf[SQLException])
  def rollback() {
    if (isLive && !_connection.getAutoCommit) _connection.rollback
    // @todo invalidate relations cache
    close()
  }

  /**
   * Close the underlying connection and dispose of any resources associated with this
   * transaction.
   */
  @throws(classOf[SQLException])
  def close() {
    if (isLive) _connection.close()
    _connection = null
  }
  
  // ### Database communication methods

  // In order to ensure that cache is synchronized with transaction we must use these methods
  // to handle all communications with JDBC in a centralized way.
  //
  // The logic is pretty simple: every query that can possibly affect the data
  // in current transaction (i.e. the one that is usually called via `executeUpdate`)
  // should lead to full cache invalidation. This way we must re-read every record
  // after such manipulation -- that fits perfectly with triggers and other stuff that
  // could possibly affect more data at backend than you intended with any particular
  // query.

  // ### Cache  @TODO, use relations inner cache

  protected[orm] def key(relation: Relation[_], id: Long): String = relation.uuid + "@" + id
  protected[orm] def key(relation: Relation[_], id: Long, association: Association[_, _]): String =
    key(relation, id) + ":" + association.uuid

  protected[orm] var recordCache = new HashMap[String, Any]
  protected[orm] var inverseCache = new HashMap[String, Seq[Any]]

  def invalidateCaches: Unit = {
    recordCache = new HashMap[String, Any]
    inverseCache = new HashMap[String, Seq[Any]]
  }

  def getCachedRecord[R](relation: Relation[R], id: Long): Option[R] =
    recordCache.get(key(relation, id)).asInstanceOf[Option[R]]

  def updateRecordCache[R](relation: Relation[R], record: R): Unit =
    if (relation.transient_?(record)){
      throw new ORMException("Transient records cannot be cached.")
    } else {
      recordCache += key(relation, relation.idOf(record).get) -> record
    }

  def evictRecordCache[R](relation: Relation[R], record: R): Unit =
    if (!relation.transient_?(record))
      recordCache -= key(relation, relation.idOf(record).get)

  def getCachedInverse[P, C](record: P, association: Association[C, P]): Seq[C] =
    if (association.foreignRelation.transient_?(record)){
      throw new ORMException("Could not retrieve inverse association for transient record.")
    } else {
      inverseCache.get(key(association.foreignRelation, association.foreignRelation.idOf(record).get, association))
      .getOrElse(null)
      .asInstanceOf[Seq[C]]
    }

  def getCachedInverse[P, C](record: P, inverse: InverseAssociation[P, C]): Seq[C] =
    getCachedInverse(record, inverse.association)

  def updateInverseCache[P, C](record: P, association: Association[C, P], children: Seq[C]): Unit =
    if (association.foreignRelation.transient_?(record)) {
      throw new ORMException("Could not update inverse association cache for transient record.")
    } else inverseCache += key(association.foreignRelation, association.foreignRelation.idOf(record).get, association) -> children

  def updateInverseCache[P, C](record: P, inverse: InverseAssociation[P, C], children: Seq[C]): Unit =
    updateInverseCache(record, inverse.association, children)

  def evictInverseCache[P, C](record: P, association: Association[C, P]): Unit =
    if (association.foreignRelation.transient_?(record)) {
      throw new ORMException("Could not evict inverse association cache for transient record.")
    } else inverseCache -= key(association.foreignRelation, association.foreignRelation.idOf(record).get, association)

  def evictInverseCache[P, C](record: P, inverse: InverseAssociation[P, C]): Unit =
    evictInverseCache(record, inverse.association)
}
