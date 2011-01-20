package ru.circumflex.orm

import xml._

class Country extends Record[Country] {
  // Constructor shortcuts
  def this(code: String, name: String) = {
    this()
    this.code = code
    this.name = name
  }
  // Fields
  var code: String = _// = "code" VARCHAR(2) DEFAULT("'ch'")
  var name: String = _// = "name" TEXT
  // Miscellaneous
  override def toString = name
}

object Country extends Table[Country] {
  val code = "code" VARCHAR(2) DEFAULT("'ch'")
  val name = "name" TEXT() DEFAULT("'ch'")
  // Inverse associations
  //val cities = inverse(City.country)

  val codeIdx = "country_code_idx" INDEX("LOWER(code)") USING "btree" UNIQUE

  // Validations
//  validation.notEmpty(code)
//      .notEmpty(name)
//      .pattern(code, "(?i:[a-z]{2})")
}

class City extends Record[City] {
  // Constructor shortcuts
  def this(country: Country, name: String) = {
    this()
    this.country = country
    this.name = name
  }
  // Fields
  var name: String = _ //"name" TEXT
  // Associations
  var country: Country = _ // = "country_id" REFERENCES(Country) ON_DELETE CASCADE ON_UPDATE CASCADE
  // Miscellaneous
  override def toString = name
}

object City extends Table[City] {
  // Fields
  val name = "name" TEXT
  // Associations
  val country = "country_id" BIGINT() REFERENCES(Country) ON_DELETE CASCADE ON_UPDATE CASCADE

  // Validations
//  validation.notEmpty(name)
//      .notNull(country.field)
}

class Capital extends Record[Capital] {
  // Constructor shortcuts
  def this(country: Country, city: City) = {
    this()
    this.country = country
    this.city = city
  }
  // Associations
  var country: Country = _ // = "country_id" REFERENCES(Country) ON_DELETE CASCADE
  var city: City = _// = "city_id" REFERENCES(City) ON_DELETE RESTRICT
}

object Capital extends Table[Capital] {
  // Associations
  val country = "country_id".BIGINT REFERENCES(Country) ON_DELETE CASCADE
  val city = "city_id" BIGINT() REFERENCES(City) ON_DELETE RESTRICT
  val countryKey = UNIQUE(country)
  val cityKey = UNIQUE(city)
}

object Sample {
  def schema = new DDLUnit(City, Capital, Country).dropCreate.messages.foreach(msg => println(msg.body))
  def selects = {
    val ci = City
    val co = Country
    // Select countries with corresponding cities:
    val s1 = SELECT (co.*, ci.*) FROM (co JOIN ci) list // Seq[(Country, City)]
    // Select countries and count their cities:
    val s2 = SELECT (co.*, COUNT(ci.id)) FROM (co JOIN ci) GROUP_BY (co.*) list // Seq[(Country, Int)]
    // Select all russian cities:
    val s3 = SELECT (ci.*) FROM (ci JOIN co) WHERE (co.code LIKE "ru") ORDER_BY (ci.name ASC) list  // Seq[City]
  }
//  def data = Deployment
//      .readAll(XML.load(getClass.getResourceAsStream("/test.cxd.xml")))
//      .foreach(_.process)
}
