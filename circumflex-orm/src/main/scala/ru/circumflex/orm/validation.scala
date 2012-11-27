package ru.circumflex.orm


// ## Validation

case class ValidationError(val source: String,
                           val errorKey: String,
                           var params: Map[String, String]) {
  def this(source: String, errorKey: String, params: Pair[String, String]*) =
    this(source, errorKey, Map(params: _*))

  params += "src" -> source

  def toMsg(messages: Messages): String =
    messages.get(source + "." + errorKey, params) match {
      case Some(m) => m
      case None => messages.get(errorKey, params) match {
          case Some(m) => m
          case None => errorKey
        }
    }
  // def toMsg(): String = toMsg(CircumflexContext.get.messages)

  override def hashCode = source.hashCode * 31 + errorKey.hashCode
  override def equals(that: Any) = that match {
    case e: ValidationError => e.source == this.source && e.errorKey == this.errorKey
    case _ => false
  }
}

class ValidationException(val errors: ValidationError *)
extends ORMException("Validation failed.") {
  val bySource = groupBy[String, ValidationError](errors, e => e.source)
  val byKey = groupBy[String, ValidationError](errors, e => e.errorKey)
}

class RecordValidator[R](relation: Relation[R]) {
  protected var _validators: Seq[R => Option[ValidationError]] = Nil
  def validators = _validators
  def validate(record: R): Seq[ValidationError] =
    _validators.flatMap(_.apply(record)).toList.removeDuplicates
  
  def add(validator: R => Option[ValidationError]): this.type = {
    _validators ++= List(validator)
    this
  }
  
  def notNull(field: Field[R, _]): this.type =
    add(r => relation.fields.find(f => f == field) match {
        case Some(f: Field[R, _]) =>
          if (f.null_?(r)) Some(new ValidationError(f.uuid, "null"))
          else None
        case _ =>
          throw new ORMException("Field " + field + " does not correspond to record " + r)
      })
  
  def notEmpty(field: TextField[R]): this.type =
    add(r => relation.fields.find(f => f == field) match {
        case Some(f: TextField[R]) =>
          if (f.getValue(r) == null) Some(new ValidationError(f.uuid, "null"))
          else if (f.getValue(r).trim == "") Some(new ValidationError(f.uuid, "empty"))
          else None
        case _ =>
          throw new ORMException("Field " + field + " does not correspond to record " + r)
      })
  
  def pattern(field: TextField[R], regex: String, key: String = "pattern"): this.type =
    add(r => relation.fields.find(f => f == field) match {
        case Some(f: TextField[R]) =>
          if (f.getValue(r) == null) Some(new ValidationError(f.uuid, "null"))
          else if (!f.getValue(r).matches(regex))
            Some(new ValidationError(f.uuid, key, "regex" -> regex, "value" -> f.getValue(r)))
          else None
        case _ =>
          throw new ORMException("Field " + field + " does not correspond to record " + r)
      })
}



