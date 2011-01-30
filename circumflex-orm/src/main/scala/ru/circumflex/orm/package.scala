package ru.circumflex


import ru.circumflex.orm.avro.AvroNode
import java.util.regex.Pattern

// ## ORM package object

package object orm {
  import ru.circumflex.orm.ORM._

  def tx = transactionManager.getTransaction
  def TX = tx
  def COMMIT() = tx.commit()
  def ROLLBACK() = tx.rollback()

  // ### Implicits

  implicit def relation2node[R](relation: Relation[R]): RelationNode[R] =
    relation.as(relation.relationName)
  implicit def node2Relation[R](node: RelationNode[R]): Relation[R] = {
    lastAlias(node.alias) // is this necessary ?
    node.relation
  }
  implicit def string2helper(expression: String): SimpleExpressionHelper =
    new SimpleExpressionHelper(expression)
  implicit def string2predicate(expression: String): Predicate =
    new SimpleExpression(expression, Nil)
  implicit def string2order(expression: String): Order =
    new Order(expression, Nil)
  implicit def paramExpr2predicate(expression: ParameterizedExpression): Predicate =
    new SimpleExpression(expression.toSql, expression.parameters)
  implicit def predicate2aggregateHelper(predicate: Predicate) =
    new AggregatePredicateHelper(predicate)
  implicit def predicate2string(predicate: Predicate): String = predicate.toInlineSql
  implicit def string2projection(expression: String): Projection[Any] =
    new ExpressionProjection[Any](expression)
  implicit def association2field[R](association: Association[R, _]): Field[R, Long] =
    association.field
  /* implicit def relation2recordSample[R](relation: Relation[R]): R =
   relation.r */
  implicit def field2projection[T](field: Field[_, T]): Projection[T] =
    new ExpressionProjection[T](field2str(field))
  // The most magical ones.
  /* implicit def node2record[R](node: RelationNode[R]): R = {
   lastAlias(node.alias)
   return node.relation.r
   } */
  /* implicit def field2str(field: Field[_]): String = lastAlias match {
   case Some(alias) => alias + "." + field.name
   case None => field.name
   } */
  implicit def field2str(field: Field[_, _]): String =
    field.relation.relationName + "." + field.name
  implicit def field2helper(field: Field[_, _]) = new SimpleExpressionHelper(field2str(field))
  implicit def field2order(field: Field[_, _]): Order = new Order(field2str(field), Nil)

  implicit def tuple2proj[T1, T2](
    t: Tuple2[Projection[T1],Projection[T2]]) =
      new Tuple2Projection(t._1, t._2)
  implicit def tuple3proj[T1, T2, T3](
    t: Tuple3[Projection[T1], Projection[T2], Projection[T3]]) =
      new Tuple3Projection(t._1, t._2, t._3)
  implicit def tuple4proj[T1, T2, T3, T4](
    t: Tuple4[Projection[T1], Projection[T2], Projection[T3], Projection[T4]]) =
      new Tuple4Projection(t._1, t._2, t._3, t._4)
  implicit def tuple5proj[T1, T2, T3, T4, T5](
    t: Tuple5[Projection[T1], Projection[T2], Projection[T3], Projection[T4], Projection[T5]]) =
      new Tuple5Projection(t._1, t._2, t._3, t._4, t._5)
  implicit def tuple6proj[T1, T2, T3, T4, T5, T6](
    t: Tuple6[Projection[T1], Projection[T2], Projection[T3], Projection[T4], Projection[T5], Projection[T6]]) =
      new Tuple6Projection(t._1, t._2, t._3, t._4, t._5, t._6)
  implicit def tuple7proj[T1, T2, T3, T4, T5, T6, T7](
    t: Tuple7[Projection[T1], Projection[T2], Projection[T3], Projection[T4], Projection[T5], Projection[T6], Projection[T7]]) =
      new Tuple7Projection(t._1, t._2, t._3, t._4, t._5, t._6, t._7)
  implicit def tuple8proj[T1, T2, T3, T4, T5, T6, T7, T8](
    t: Tuple8[Projection[T1], Projection[T2], Projection[T3], Projection[T4], Projection[T5], Projection[T6], Projection[T7], Projection[T8]]) =
      new Tuple8Projection(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8)
  implicit def tuple9proj[T1, T2, T3, T4, T5, T6, T7, T8, T9](
    t: Tuple9[Projection[T1], Projection[T2], Projection[T3], Projection[T4], Projection[T5], Projection[T6], Projection[T7], Projection[T8], Projection[T9]]) =
      new Tuple9Projection(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9)
  implicit def tuple10proj[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](
    t: Tuple10[Projection[T1], Projection[T2], Projection[T3], Projection[T4], Projection[T5], Projection[T6], Projection[T7], Projection[T8], Projection[T9], Projection[T10]]) =
      new Tuple10Projection(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)

  // Constants

  val NO_ACTION = ForeignKeyAction(dialect.fkNoAction)
  val CASCADE = ForeignKeyAction(dialect.fkCascade)
  val RESTRICT = ForeignKeyAction(dialect.fkRestrict)
  val SET_NULL = ForeignKeyAction(dialect.fkSetNull)
  val SET_DEFAULT = ForeignKeyAction(dialect.fkSetDefault)

  val INNER = JoinType(dialect.innerJoin)
  val LEFT = JoinType(dialect.leftJoin)
  val RIGHT = JoinType(dialect.rightJoin)
  val FULL = JoinType(dialect.fullJoin)

  val OP_UNION = SetOperation(dialect.union)
  val OP_UNION_ALL = SetOperation(dialect.unionAll)
  val OP_EXCEPT = SetOperation(dialect.except)
  val OP_EXCEPT_ALL = SetOperation(dialect.exceptAll)
  val OP_INTERSECT = SetOperation(dialect.intersect)
  val OP_INTERSECT_ALL = SetOperation(dialect.intersectAll)

  // Predicates.

  def AND(predicates: Predicate*) =
    new AggregatePredicateHelper(predicates.head).and(predicates.tail: _*)
  def OR(predicates: Predicate*) =
    new AggregatePredicateHelper(predicates.head).or(predicates.tail: _*)
  def NOT(predicate: Predicate) =
    new SimpleExpression(dialect.not(predicate.toSql), predicate.parameters)
  def expr[T](expression: String): ExpressionProjection[T] =
    new ExpressionProjection[T](expression)
  def prepareExpr(expression: String, params: Pair[String, Any]*): SimpleExpression = {
    var sqlText = expression
    var parameters: Seq[Any] = Nil
    val paramsMap = Map[String, Any](params: _*)
    val pattern = Pattern.compile(":(\\w+)\\b")
    val matcher = pattern.matcher(expression)
    while(matcher.find) paramsMap.get(matcher.group(1)) match {
      case Some(param) => parameters ++= List(param)
      case _ => parameters ++= List(null)
    }
    sqlText = matcher.replaceAll("?")
    new SimpleExpression(sqlText, parameters)
  }

  // Simple subqueries.

  def EXISTS(subquery: SQLQuery[_]) =
    new SubqueryExpression(dialect.exists, subquery)
  def NOT_EXISTS(subquery: SQLQuery[_]) =
    new SubqueryExpression(dialect.notExists, subquery)

  // Simple projections.

  def COUNT(expr: String) =
    new ExpressionProjection[Int](dialect.count + "(" + expr + ")")
  def COUNT_DISTINCT(expr: String) =
    new ExpressionProjection[Int](dialect.count + "(" + dialect.distinct + " " + expr + ")")
  def MAX(expr: String) =
    new ExpressionProjection[Any](dialect.max + "(" + expr + ")")
  def MIN(expr: String) =
    new ExpressionProjection[Any](dialect.min + "(" + expr + ")")
  def SUM(expr: String) =
    new ExpressionProjection[Any](dialect.sum + "(" + expr + ")")
  def AVG(expr: String) =
    new ExpressionProjection[Any](dialect.avg + "(" + expr + ")")

  // Query DSLs

  def SELECT[R](projection: Projection[R]) = new Select(projection)
  def INSERT_INTO[R](relation: Relation[R]) = new InsertSelectHelper(relation)
  def UPDATE[R](node: RelationNode[R]) = new Update(node)
  def DELETE[R](node: RelationNode[R]) = new Delete(node)


  // functions
  
  def AVRO[R](relation: Relation[R], fileName: String) = new AvroNode[R](relation, fileName)
}
