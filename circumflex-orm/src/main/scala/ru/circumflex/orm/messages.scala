package ru.circumflex.orm


import java.util.Locale
import java.util.ResourceBundle

class Messages(val baseName: String, val locale: Locale) extends HashModel[String] {
  private val log = ORM.getLogger(this) 

  val msgBundle: ResourceBundle = try {
    ResourceBundle.getBundle(baseName, locale)
  } catch {
    case e: Throwable => {
        log.warning("ResourceBundle for messages instance not found: " + baseName)
				null
			}
  }
  def get(key: String): Option[String] = try {
    Option(msgBundle.getString(key))
  } catch {
    case e: Throwable => None
  }
  def get(key: String, params: Pair[String, String]*): Option[String] = get(key, Map(params: _*))
  def get(key: String, params: Map[String, String]): Option[String] =
    get(key).map(m => params.foldLeft(m) {
				case (m, (name, value)) => m.replaceAll("\\{" + name + "\\}", value)
			})
  override def apply(key: String): String = get(key) match {
    case Some(v) => v
    case _ =>
      log.warning("Missing message for key %s, locale %s.".format(key, msgBundle.getLocale))
      ""
  }
  def apply(key: String, params: Pair[String, String]*): String = apply(key, Map(params: _*))
  def apply(key: String, params: Map[String, String]): String = get(key, params) match {
    case Some(v) => v
    case _ =>
      log.warning("Missing message for key %s, locale %s.".format(key, msgBundle.getLocale))
      ""
  }
}
