package ru.circumflex.orm

import java.lang.reflect.Method
import java.lang.reflect.Modifier
import java.math.BigDecimal
import java.math.BigInteger
import java.util.ArrayList

/**
 * R: Type of record class
 * T: Type of class's variable
 */
final case class ClassVariable[R, T](name: String, getter: Method, setter: Method) {
  if (getter != null) getter.setAccessible(true)
  if (setter != null) setter.setAccessible(true)

  def getValue(from: R): T = {
    try {
      getter.invoke(from).asInstanceOf[T]
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }

  def setValue(to: R, value: T) {
    try {
      setter.invoke(to, value.asInstanceOf[AnyRef]) // @todo, T is any
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }

  def copyField(from: R, to: R) {
    setValue(to, getValue(from))
  }
}

object ClassUtil {
  
  @volatile private var serialNo: Long = _

  private val MAKE_ACCESSIBLE = true

  val BooleanClass = classOf[scala.Boolean]
  val ByteClass = classOf[scala.Byte]
  val ShortClass = classOf[scala.Short]
  val IntClass = classOf[scala.Int]
  val LongClass = classOf[scala.Long]
  val FloatClass = classOf[scala.Float]
  val DoubleClass = classOf[scala.Double]

  val JBooleanClass = classOf[java.lang.Boolean]
  val JByteClass = classOf[java.lang.Byte]
  val JShortClass = classOf[java.lang.Short]
  val JIntegerClass = classOf[java.lang.Integer]
  val JLongClass = classOf[java.lang.Long]
  val JFloatClass = classOf[java.lang.Float]
  val JDoubleClass = classOf[java.lang.Double]

  val StringClass = classOf[String]

  val BigDecimalClass = classOf[java.math.BigDecimal]
  val BigIntegerClass = classOf[java.math.BigInteger]
  val DateClass = classOf[java.util.Date]
  val SqlDateClass = classOf[java.sql.Date]
  val SqlTimeClass = classOf[java.sql.Time]
  val SqlTimestampClass = classOf[java.sql.Timestamp]

  val JListClass = classOf[java.util.List[_]]


  def newObject[T](clazz: Class[T]): T = {
    // must create new instances
    clazz match {
      case IntClass =>
        serialNo += 1
        new java.lang.Integer(serialNo.toShort).asInstanceOf[T]
      case StringClass =>
        serialNo += 1
        ("" + serialNo).asInstanceOf[T]
      case LongClass =>
        serialNo += 1
        new java.lang.Long(serialNo).asInstanceOf[T]
      case ShortClass =>
        serialNo += 1
        new java.lang.Short(serialNo.toShort).asInstanceOf[T]
      case ByteClass =>
        serialNo += 1
        new java.lang.Byte(serialNo.toByte).asInstanceOf[T]
      case FloatClass =>
        serialNo += 1
        new java.lang.Float(serialNo).asInstanceOf[T]
      case DoubleClass =>
        serialNo += 1
        new java.lang.Double(serialNo).asInstanceOf[T]
      case BooleanClass =>
        serialNo += 1
        new java.lang.Boolean(false).asInstanceOf[T]
      case BigDecimalClass =>
        serialNo += 1
        new BigDecimal(serialNo).asInstanceOf[T]
      case BigIntegerClass =>
        serialNo += 1
        new BigInteger("" + serialNo).asInstanceOf[T]
      case SqlDateClass =>
        serialNo += 1
        new java.sql.Date(serialNo).asInstanceOf[T]
      case SqlTimeClass =>
        serialNo += 1
        return new java.sql.Time(serialNo).asInstanceOf[T]
      case SqlTimestampClass =>
        serialNo += 1
        new java.sql.Timestamp(serialNo).asInstanceOf[T]
      case DateClass =>
        serialNo += 1
        new java.util.Date(serialNo).asInstanceOf[T]
      case JListClass =>
        new ArrayList[T].asInstanceOf[T]
      case _ =>
        try {
          return clazz.newInstance
        } catch {case e: Exception =>
            if (MAKE_ACCESSIBLE) {
              val constructors = clazz.getDeclaredConstructors
              // try 0 length constructors
              for (c <- constructors) {
                if (c.getParameterTypes.length == 0) {
                  c.setAccessible(true)
                  try {
                    return clazz.newInstance
                  } catch {case e: Exception =>}
                }
              }
              // try 1 length constructors
              for (c <- constructors) {
                if (c.getParameterTypes.length == 1) {
                  c.setAccessible(true)
                  try {
                    return c.newInstance(new Array[Object](1)).asInstanceOf[T]
                  } catch {case e: Exception =>}
                }
              }
            }
            throw new RuntimeException("Exception trying to create " + clazz.getName + ": " + e, e)
        }
    }

  }

  def toSqlType(jvmType: Class[_]): String = {
    jvmType match {
      case BooleanClass | JBooleanClass => "TINYINT"
      case IntClass | JIntegerClass => "INT"
      case LongClass | JLongClass => "BIGINT"
      case DoubleClass | JDoubleClass => "DOUBLE"
      case BigDecimalClass => "DECIMAL"
      case DateClass => "DATE"
      case SqlDateClass => "DATE"
      case SqlTimeClass => "TIME"
      case SqlTimestampClass => "TIMESTAMP"
      case _ =>  "VARCHAR"
    }
  }

  def isSimpleType[T](clazz: Class[T]): java.lang.Boolean = {
    if (classOf[Number].isAssignableFrom(clazz)) {
      true
    } else if (clazz == classOf[String]) {
      true
    } else false
  }

  def getValDefs(clz: Class[_]): Array[Method] =
    clz.getMethods filter isValDef(clz)

  def isValDef(clz: Class[_])(m: Method): Boolean = {
    if (clz.getDeclaredFields.exists(f => f.getName == m.getName && f.getType == m.getReturnType)) {
      true
    } else {
      if (clz != classOf[Any]) {
        // getDeclaredFields does not include inherited fields
        isValDef(clz.getSuperclass)(m)
      } else false
    }
  }

  def getPublicVariables[R](clz: Class[R]): List[ClassVariable[R, _]] = {
    var fields: List[ClassVariable[R, _]] = Nil
    val methods = clz.getMethods
    for (method <- methods; if Modifier.isPublic(method.getModifiers)) {
      val name = method.getName
      val params = method.getParameterTypes
      if (name.endsWith("_$eq") && params.length == 1) {
        val getterName = name.substring(0, name.length - 4)
        val paramType = params(0)
        methods find (x => x.getName == getterName && x.getReturnType == paramType) match {
          case Some(getter) => fields ::= ClassVariable(getterName, getter, method)
          case _ =>
        }
      }
    }
    fields
  }

  def convert(a: AnyRef, targetType: Class[_]): AnyRef = {
    if (a == null)
      return null

    val currentType = a.getClass
    if (targetType.isAssignableFrom(currentType))
      return a

    if (targetType == StringClass)
      return a.toString

    if (classOf[Number].isAssignableFrom(currentType)) {
      val n = a.asInstanceOf[Number]
      targetType match {
        case IntClass =>
          return new java.lang.Integer(n.intValue)
        case LongClass =>
          return new java.lang.Long(n.longValue)
        case FloatClass =>
          return new java.lang.Float(n.floatValue)
        case DoubleClass =>
          return new java.lang.Double(n.doubleValue)
        case _ =>
      }
    }

    throw new RuntimeException("Can not convert the value " + a +
                               " from " + currentType + " to " + targetType)
  }


}
