package ru.circumflex.orm

import xml._


object Model {

  class Country {
    // Constructor shortcuts
    def this(code: String, name: String) = {
      this()
      this.code = code
      this.name = name
    }
    // Fields
    var code: String = _
    var name: String = _
    var capital: Capital = _
    var cities: List[City] = Nil
    //
    // Miscellaneous
    override def toString = "Country(name=" + name + ", code=" + code + ")"
  }

  object Countries extends Table[Country] {
    val code = "code" VARCHAR(2) DEFAULT("'ch'")
    val name = "name" TEXT
    val capital = "capital_id".BIGINT REFERENCES(Capitals)

    // Inverse associations, should be def or lazy val
    def cities = inverse(Cities.country)

    val codeIdx = "country_code_idx" INDEX("LOWER(code)") USING "btree" UNIQUE

    // Validations
//  validation.notEmpty(code)
//      .notEmpty(name)
//      .pattern(code, "(?i:[a-z]{2})")
  }

  class City {
    // Constructor shortcuts
    def this(country: Country, name: String) = {
      this()
      this.country = country
      this.name = name
    }
    var name: String = _
    var country: Country = _
    var serialized: Array[Double] = Array(1, 2, 3)
    override def toString = "City(name=" + name + " serialized=" + (serialized mkString (",")) + ")"
  }

  object Cities extends Table[City] {
    // Fields
    val name = "name" TEXT
    // Associations
    val country = "country_id".BIGINT REFERENCES(Countries) ON_DELETE CASCADE ON_UPDATE CASCADE
    val serialized = "serialized" SERIALIZED(classOf[Array[Double]], 100)

    // Validations
//  validation.notEmpty(name)
//      .notNull(country.field)
  }

  class Capital {
    // Constructor shortcuts
    def this(country: Country, city: City) = {
      this()
      this.country = country
      this.city = city
    }
    // Associations
    var country: Country = _
    var city: City = _
    
    override def toString = "Capital(country=" + country.name + ", name=" + city.name + ")"
  }

  object Capitals extends Table[Capital] {
    // Associations
    val country = "countries_id".BIGINT REFERENCES(Countries) ON_DELETE CASCADE
    val city = "cities_id".BIGINT REFERENCES(Cities) ON_DELETE RESTRICT
    val countryKey = UNIQUE(country)
    val cityKey = UNIQUE(city)
  }


  def main(args: Array[String]) {
    schema
    inserts
    selects
  }

  def schema = new DDLUnit(Cities, Capitals, Countries).dropCreate.messages.foreach(msg => println(msg.body))

  def inserts = {
    val country = new Country("ru", "russian")
    Countries.save(country)
    println(Countries.idOf(country) + " " + country)

    val city = new City(country, "mossco")
    Cities.save(city)
    Cities.save(new City(country, "abc"))
    Cities.save(new City(country, "def"))
    
    val capital = new Capital(country, city)
    Capitals.save(capital)
    country.capital = capital
    Countries.save(country)

    println(Countries.idOf(country) + " " + country)
  }

  def selects = {
    val co = Countries
    val ca = Capitals
    val ci = Cities
    // Select countries with corresponding cities:
    val s1 = SELECT (co.*, ca.*) FROM (co JOIN ca) list // Seq[(Country, City)]

    var country1: Country = null
    s1 foreach {case (country, capital) =>
        println("capital " + capital + "'s country" + " is " + capital.country)
        println("country " + country + "'s capital" + " is " + country.capital)
        country1 = country
    }

    //def n2r[R <: AnyRef](n: RelationNode[R]): T forSome {type T <: Relation[_]} = n.relation
    //n2r(co)

    // Select countries and count their cities:
    val s2 = SELECT (co.*, COUNT(ci.id)) FROM (co JOIN ci) GROUP_BY (co.*) list // Seq[(Country, Int)]
    // Select all russian cities:
    val s3 = SELECT (ci.*) FROM (ci JOIN co) WHERE (co.code LIKE "ru") ORDER_BY (ci.name ASC) list  // Seq[City]
    //val s4 = SELECT (City.*) FROM (City JOIN Country) WHERE (Country.code LIKE "ru") ORDER_BY (City.name ASC) list  // Seq[City]

    country1.cities ++= Countries.cities(country1)
    country1.cities foreach println
  }
  
}