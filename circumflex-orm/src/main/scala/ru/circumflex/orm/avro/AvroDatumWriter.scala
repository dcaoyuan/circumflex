package ru.circumflex.orm.avro

import java.io.IOException
import java.nio.ByteBuffer
import org.apache.avro.AvroTypeException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.Encoder
import scala.collection.JavaConversions._

object AvroDatumWriter {
  def apply[R]() = new AvroDatumWriter[R](null, GenericData.get)
}

class AvroDatumWriter[R] private (private var _root: Schema, data: GenericData) extends DatumWriter[R] {

  setSchema(_root)
  //public GenericDatumWriter() { this(GenericData.get()); }

  //protected GenericDatumWriter(GenericData data) { this.data = data; }


//  protected GenericDatumWriter(Schema root, GenericData data) {
//    this(data);
//    setSchema(root);
//  }

  def setSchema(root: Schema) {
    this._root = root
  }

  @throws(classOf[IOException])
  def write(datum: R, out: Encoder) {
    write(_root, datum, out)
  }

  /** Called to write data.*/
  @throws(classOf[IOException])
  protected def write(schema: Schema, datum: Any, out: Encoder) {
    try {
      import Schema.Type._
      schema.getType match {
        case RECORD => writeRecord(schema, datum.asInstanceOf[R], out)
        case ENUM =>   writeEnum(schema, datum.asInstanceOf[AnyRef], out)
        case ARRAY =>  writeArray(schema, datum.asInstanceOf[AnyRef], out)
        case MAP =>    writeMap(schema, datum.asInstanceOf[AnyRef], out)
        case UNION =>
          val index = data.resolveUnion(schema, datum)
          out.writeIndex(index)
          write(schema.getTypes.get(index), datum, out)
        case FIXED =>   writeFixed(schema, datum.asInstanceOf[AnyRef], out)
        case STRING =>  writeString(schema, datum.asInstanceOf[AnyRef], out)
        case BYTES =>   writeBytes(datum.asInstanceOf[AnyRef], out)
        case INT =>     out.writeInt(datum.asInstanceOf[Int])
        case LONG =>    out.writeLong(datum.asInstanceOf[Long])
        case FLOAT =>   out.writeFloat(datum.asInstanceOf[Float])
        case DOUBLE =>  out.writeDouble(datum.asInstanceOf[Double])
        case BOOLEAN => out.writeBoolean(datum.asInstanceOf[Boolean])
        case NULL =>    out.writeNull
        case _ => error(schema, datum)
      }
    } catch {
      case ex: NullPointerException => throw npe(ex, " of "+schema.getName)
    }
  }

  private def npe(e: NullPointerException, s: String): NullPointerException = {
    val result = new NullPointerException(e.getMessage + s)
    result.initCause(Option(e.getCause) getOrElse e)
    result
  }

  /** Called to write a record.  May be overridden for alternate record
   * representations.*/
  @throws(classOf[IOException])
  protected def writeRecord(schema: Schema, datum: R, out: Encoder) {
    for (field <- schema.getFields) {
      val value = getField(datum, field.name, field.pos)
      try {
        write(field.schema, value, out)
      } catch {
        case e: NullPointerException => throw npe(e, " in field "+field.name)
      }
    }
  }

  /** Called by the default implementation of {@link #writeRecord} to retrieve
   * a record field value.  The default implementation is for {@link
   * IndexedRecord}.*/
  protected def getField(record: Any, field: String, position: Int): AnyRef = {
    record.asInstanceOf[IndexedRecord].get(position)
  }

  /** Called to write an enum value.  May be overridden for alternate enum
   * representations.*/
  @throws(classOf[IOException])
  protected def writeEnum(schema: Schema, datum: AnyRef, out: Encoder) {
    out.writeEnum(schema.getEnumOrdinal(datum.toString))
  }

  /** Called to write a array.  May be overridden for alternate array
   * representations.*/
  @throws(classOf[IOException])
  protected def writeArray(schema: Schema, datum: AnyRef, out: Encoder) {
    val element = schema.getElementType
    val size = getArraySize(datum)
    out.writeArrayStart()
    out.setItemCount(size)
    val itr = getArrayElements(datum)
    while (itr.hasNext) {
      out.startItem
      write(element, itr.next, out)
    }
    out.writeArrayEnd
  }

  /** Called by the default implementation of {@link #writeArray} to get the
   * size of an array.  The default implementation is for {@link Collection}.*/
  protected def getArraySize(array: AnyRef): Long = {
    array.asInstanceOf[java.util.Collection[_]].size
  }

  /** Called by the default implementation of {@link #writeArray} to enumerate
   * array elements.  The default implementation is for {@link Collection}.*/
  protected def getArrayElements(array: AnyRef): java.util.Iterator[_ <: AnyRef] = {
    array.asInstanceOf[java.util.Collection[AnyRef]].iterator
  }

  /** Called to write a map.  May be overridden for alternate map
   * representations.*/
  @throws(classOf[IOException])
  protected def writeMap(schema: Schema, datum: AnyRef, out: Encoder) {
    val value = schema.getValueType
    val size = getMapSize(datum)
    out.writeMapStart
    out.setItemCount(size)
    for (entry <- getMapEntries(datum)) {
      out.startItem
      writeString(entry.getKey, out)
      write(value, entry.getValue, out)
    }
    out.writeMapEnd
  }

  /** Called by the default implementation of {@link #writeMap} to get the size
   * of a map.  The default implementation is for {@link Map}.*/
  protected def getMapSize(map: AnyRef): Int = {
    map.asInstanceOf[java.util.Map[_, _]].size
  }

  /** Called by the default implementation of {@link #writeMap} to enumerate
   * map elements.  The default implementation is for {@link Map}.*/
  protected def getMapEntries(map: AnyRef): java.lang.Iterable[java.util.Map.Entry[AnyRef,AnyRef]] = {
    map.asInstanceOf[java.util.Map[AnyRef, AnyRef]].entrySet
  }

  /** Called to write a string.  May be overridden for alternate string
   * representations.*/
  @throws(classOf[IOException])
  protected def writeString(schema: Schema, datum: AnyRef, out: Encoder) {
    writeString(datum, out)
  }
  /** Called to write a string.  May be overridden for alternate string
   * representations.*/
  @throws(classOf[IOException])
  protected def writeString(datum: AnyRef, out: Encoder) {
    out.writeString(datum.asInstanceOf[CharSequence])
  }

  /** Called to write a bytes.  May be overridden for alternate bytes
   * representations.*/
  @throws(classOf[IOException])
  protected def writeBytes(datum: AnyRef, out: Encoder) {
    datum match {
      case null => out.writeBytes(null.asInstanceOf[ByteBuffer])
      case x: ByteBuffer => out.writeBytes(x)
      case x: Array[Byte] => out.writeBytes(x)
    }
  }

  /** Called to write a fixed value.  May be overridden for alternate fixed
   * representations.*/
  @throws(classOf[IOException])
  protected def writeFixed(schema: Schema, datum: AnyRef, out: Encoder) {
    out.writeFixed(datum.asInstanceOf[GenericFixed].bytes, 0, schema.getFixedSize)
  }

  private def error(schema: Schema, datum: Any) {
    throw new AvroTypeException("Not a "+schema+": "+datum)
  }

}

