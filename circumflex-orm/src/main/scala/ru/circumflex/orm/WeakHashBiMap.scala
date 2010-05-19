package ru.circumflex.orm

import scala.collection.generic.CanBuildFrom
import scala.collection.generic.MutableMapFactory
import scala.collection.mutable.DefaultEntry
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.mutable.MapLike

object WeakHashBiMap extends MutableMapFactory[WeakHashBiMap] {
  implicit def canBuildFrom[A, B]: CanBuildFrom[Coll, (A, B), WeakHashBiMap[A, B]] = new MapCanBuildFrom[A, B]
  def empty[A, B]: WeakHashBiMap[A, B] = new WeakHashBiMap[A, B]
}

@serializable @SerialVersionUID(1L)
class WeakHashBiMap[A, B](forward: HashMap[A, B], backward: HashMap[B, A]
) extends Map[A, B]
     with MapLike[A, B, WeakHashBiMap[A, B]] {

  def this() = this(new HashMap[A, B], new HashMap[B, A])

  type Entry = DefaultEntry[A, B]

  private lazy val inverseOne = new WeakHashBiMap(backward, forward)

  def inverse: WeakHashBiMap[B, A] = inverseOne

  override def empty: WeakHashBiMap[A, B] = WeakHashBiMap.empty[A, B]
  override def clear = {
    forward.clear
    backward.clear
  }
  override def size: Int = forward.size

  def get(key: A): Option[B] = {
    forward.get(key)
  }

  override def put(key: A, value: B): Option[B] = {
    backward.put(value, key)
    forward.put(key, value)
  }

  override def update(key: A, value: B): Unit = put(key, value)

  override def remove(key: A): Option[B] = {
    forward.get(key) match {
      case Some(v) => backward.remove(v)
      case None =>
    }
    forward.remove(key)
  }

  def +=(kv: (A, B)): this.type = {
    forward += kv
    backward += (kv._2 -> kv._1)
    this
  }

  def -=(key: A): this.type = {
    forward.get(key) match {
      case Some(v) => backward -= v
      case None =>
    }

    forward -= key
    this
  }

  def iterator = {
    forward.iterator
  }

  override def foreach[C](f: ((A, B)) => C) {
    forward.foreach(f)
  }

  override def keySet: collection.Set[A] = {
    forward.keySet
  }

  override def values: collection.Iterable[B] = {
    forward.values
  }

  override def keysIterator: Iterator[A] = {
    forward.keysIterator
  }

  override def valuesIterator: Iterator[B] = {
    forward.valuesIterator
  }

  private def writeObject(out: java.io.ObjectOutputStream) {
    out.defaultWriteObject
    out.writeObject(forward)
  }

  private def readObject(in: java.io.ObjectInputStream) {
    in.defaultReadObject
    val theForward = in.readObject.asInstanceOf[HashMap[A, B]]
    for ((k, v) <- theForward) {
      forward += (k -> v)
      backward += (v -> k)
    }
  }
}

