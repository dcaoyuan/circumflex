package ru.circumflex.orm

import java.io.File
import xml._

/*!# XML (de)serialization

 Circumflex ORM allows you to load graphs of associated records from XML files.
 This is very useful for loading test data and exchanging records between databases
 with associations preserving (in id-independent style).

 Every `Field` capable of (de)serializing itself (from)into XML should extend the
 `XmlSerializable` trait. A record can be read from XML format if it contains only
 `XmlSerializable` fields.
 */
trait XmlSerializable[T] {
  def fromXml(str: String): T
  def toXml(value: T): String
}

/**
 * Deployment is a unit of work of XML import tool. It specifies the prefix
 * for record classes resolution, as well as the behavior, if certain records
 * already exist.
 */
class Deployment(val id: String,
                 val prefix: String,
                 val onExist: Deployment.OnExistAction,
                 val entries: Seq[Node]
) {
  private val log = ORM.getLogger(this)  

  def process(): Unit = try {
    entries.foreach(e => processNode(e, Nil))
    tx.commit
  } catch {
    case e: Throwable =>
      tx.rollback
      throw e
  }

  protected def processNode[R](
    node: Node,
    parentPath: Seq[Pair[Association[_, _], Any]]): R = {
    val cl = pickClass(node)
    var r = cl.newInstance.asInstanceOf[R]
    // Decide, whether a record should be processed, and how exactly.
    if (node.attributes.next != null) {
      val crit = prepareCriteria(r, node)
      crit.unique match {
        case Some(rec) if (onExist == Deployment.Skip || node.child.size == 0) =>
          return rec
        case Some(rec) if (onExist == Deployment.Recreate) =>
          crit.mkDelete.execute()
        case Some(rec) if (onExist == Deployment.Update) =>
          r = rec
        case _ =>
      }
    }
    val relation = RelationRegistry.getRelation(r)
    // If we are still here, let's process the record further: set parents, attributes, subelements
    // and foreigners.
    parentPath.foreach{p =>
      if (relation.fields.contains(p._1.field)) "TODO"/* r.setField(p._1.field, p._2.id.getValue) */}
    var foreigns: Seq[Pair[Association[_, _], Node]] = Nil
    node.attributes.foreach(a => setRecordField(r, a.key, a.value.toString))
    node.child.foreach {
      case n: Elem => try {
          r.asInstanceOf[AnyRef].getClass.getMethod(n.label) match {
            case m if (classOf[Field[R, _]].isAssignableFrom(m.getReturnType)) =>
              setRecordField(r, n.label, n.text.trim)
            case m if (classOf[Association[_, _]].isAssignableFrom(m.getReturnType)) =>
              n.child.find(_.isInstanceOf[Elem]) match {
                case Some(n) =>
                  val a = m.invoke(r).asInstanceOf[Association[R, R]]
                  val parent = processNode(n, parentPath ++ List(a -> r))
                  // @TODO r.setValue(a, parent)
                case None =>
                  throw new ORMException("The element <" + n.label + "> is empty.")
              }
            case m if (classOf[InverseAssociation[_, _]].isAssignableFrom(m.getReturnType)) =>
              val a = m.invoke(r).asInstanceOf[InverseAssociation[R, R]].association
              foreigns ++= n.child.filter(_.isInstanceOf[Elem]).map(n => (a -> n))
          }
        } catch {
          case e: NoSuchMethodException =>
            log.warning("Could not process '" + n.label + "' of " + r.asInstanceOf[AnyRef].getClass)
        }
      case _ =>
    }
    // Now the record is ready to be saved.
    relation.save(r)
    // Finally, process the foreigners.
    foreigns.foreach(p =>
      processNode(p._2, parentPath ++ List(p._1.asInstanceOf[Association[R, R]] -> r)))
    // And return our record.
    return r
  }

  protected def pickClass(node: Node): Class[_] = {
    var p = ""
    if (prefix != "") p = prefix + "."
    return Class.forName(p + node.label, true, Thread.currentThread().getContextClassLoader())
  }

  protected def setRecordField[R](r: R, k: String, v: String): Unit = try {
    val m = r.asInstanceOf[AnyRef].getClass.getMethod(k)
    if (classOf[Field[R, _]].isAssignableFrom(m.getReturnType)) {    // only scalar fields are accepted
      val field = m.invoke(r).asInstanceOf[Field[R, Any]]
      val value = convertValue(r, k, v)
      field.setValue(r, value)
    }
  } catch {
    case e: NoSuchMethodException => log.warning("Could not process '" + k + "' of " + r.asInstanceOf[AnyRef].getClass)
  }

  protected def prepareCriteria[R](r: R, n: Node): Criteria[R] = {
    val relation = RelationRegistry.getRelation(r)
    val crit = relation.criteria
    n.attributes.foreach(a => {
        val k = a.key
        val v = convertValue(r, k, a.value.toString)
        val field = r.asInstanceOf[AnyRef].getClass.getMethod(k).invoke(r).asInstanceOf[Field[R, Any]]
        crit.add(field EQ v)
      })
    return crit
  }

  protected def convertValue(r: Any, k: String, v: String): Any = try {
    r.asInstanceOf[AnyRef].getClass.getMethod(k).invoke(r).asInstanceOf[XmlSerializable[Any]].fromXml(v)
  } catch {
    case _: Throwable => v
  }

  override def toString = id match {
    case "" => "deployment@" + hashCode
    case _ => id
  }

}

object Deployment {

  trait OnExistAction
  object Skip extends OnExistAction
  object Update extends OnExistAction
  object Recreate extends OnExistAction

  def readOne(n: Node): Deployment = if (n.label == "deployment") {
    val id = (n \ "@id").text
    val prefix = (n \ "@prefix").text
    val onExist = (n \ "@onExist").text match {
      case "keep" | "ignore" | "skip" => Deployment.Skip
      case "update" => Deployment.Update
      case "recreate" | "delete" | "delete-create" | "overwrite" => Deployment.Recreate
      case _ => Deployment.Skip
    }
    return new Deployment(id, prefix, onExist, n.child.filter(n => n.isInstanceOf[Elem]))
  } else throw new ORMException("<deployment> expected, but <" + n.label + "> found.")

  def readAll(n: Node): Seq[Deployment] = if (n.label == "deployments")
    (n \ "deployment").map(n => readOne(n))
  else throw new ORMException("<deployments> expected, but " + n.label + " found.")

}

class FileDeploymentHelper(f: File) {
  def process(): Unit = Deployment.readAll(XML.loadFile(f)).foreach(_.process)
}
