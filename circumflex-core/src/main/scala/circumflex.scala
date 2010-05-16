package ru.circumflex.core

import java.io.File
import java.util.{Locale, ResourceBundle}
import org.apache.commons.io.FilenameUtils._
import javax.servlet.http.HttpServletRequest

/**
 * Circumflex configuration
 */
object Circumflex extends HashModel {
  private val _params = MutableMap[String, Any]()

  def apply(key: String): Option[Any] = _params.get(key)
  def update(key: String, value: Any) = { _params(key) = value }

  // ### Defaults

  // Should filter process request?
  this("cx.process_?") =
      (r: HttpServletRequest) => !r.getRequestURI.toLowerCase.matches("/public/.*")

  // ### Read configuration from `cx.properties` file

  try {
    val bundle = ResourceBundle.getBundle("cx", Locale.getDefault, classLoader)
    val keys = bundle.getKeys
    while (keys.hasMoreElements) {
      val k = keys.nextElement
      this(k) = bundle.getString(k)
    }
  } catch {
    case _ => cxLog.warn("Could not read configuration parameters from cx.properties.")
  }

  // ### Parameters

  val webappRoot: File = this("cx.root") match {
    case Some(s: String) => new File(separatorsToSystem(s))
    case _ => new File(separatorsToSystem("src/main/webapp"))
  }
  val publicRoot: File = this("cx.public") match {
    case Some(s: String) => new File(webappRoot, separatorsToSystem(s))
    case _ => new File(webappRoot, "public")
  }
  def messages(locale: Locale): Option[Messages] = this("cx.messages") match {
    case Some(s: String) => Some(new Messages(s, locale))
    case _ => Some(new Messages("Messages", locale))
  }

  // ### Classloading

  def classLoader: ClassLoader = this("cx.classLoader") match {
    case Some(cld: ClassLoader) => cld
    case _ => Thread.currentThread.getContextClassLoader
  }
  def loadClass[C](name: String): Class[C] =
    Class.forName(name, true, classLoader).asInstanceOf[Class[C]]
  def newObject[C](name: String, default: =>C): C = this(name) match {
    case Some(h: C) => h
    case Some(c: Class[C]) => c.newInstance
    case Some(s: String) => loadClass[C](s).newInstance
    case _ => default
  }

}

// ## Exception

class CircumflexException(msg: String, cause: Throwable = null)
    extends Exception(msg, cause) {
  def this(cause: Throwable) = this(null, cause)
}