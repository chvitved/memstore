package com.memstore.query.parser

import org.junit.Test
import org.junit.Before
import org.junit.Assert._

class QueryTest {
  
  val parser = new QueryParser();
  
  @Test
  def testParseSimpleQuery() {
    parse("entity test where a > ?", true)
    parse("entity test", true)
    parse("entity test\n where a > ?", true)
    parse("entity	test	 where	 a>	?", true)
    parse("entity test where test.a > ?", false)
    parse("entity test where a > 3", false)
    parse("entity test where a > b", false)
    parse("""entity test where a > "b"""", false)
  }
  
  private def parse(query: String, success: Boolean) {
    var result = parser.parseAll(parser.query, query);
    assertEquals(success, result.successful)
  }
  

}
