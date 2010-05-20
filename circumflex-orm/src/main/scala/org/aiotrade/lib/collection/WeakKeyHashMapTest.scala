package org.aiotrade.lib.collection

object WeakKeyHashMapTest {

  def main(args: Array[String]) {
    var str1 = new String("newString1")
    var str2 = "literalString2"
    var str3 = "literalString3"
    var str4 = new String("newString4")
    val map = new WeakKeyHashMap[Any, Object]

    map.put(str1, new Object)
    map.put(str2, new Object)
    map.put(str3, new Object)
    map.put(str4, new Object)

    map foreach println

    /**
     * Discard the strong reference to all the keys
     */
    str1 = null
    str2 = null
    str3 = null
    str4 = null

    (0 until 20) foreach {_ =>
      System.gc
      /**
       * Verify Full GC with the -verbose:gc option
       * We expect the map to be emptied as the strong references to
       * all the keys are discarded. The map size should be 2 now
       */
      println("map.size = " + map.size + "  " + map.mkString("[", ", ", "]"))
    }
  }
}
