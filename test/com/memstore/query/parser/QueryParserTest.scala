package com.memstore.query.parser

import org.junit.Test
import org.junit.Before
import org.junit.Assert._

class QueryTest {
  
  val parser = new QueryParser();
  
  @Test
  def testParseSimpleQuery() {
    parse("entity test where a > :1", true)
    parse("entity test", true)
    parse("entity test\n where a > :1", true)
    parse("entity	test	 where	 a>	:1", true)
    parse("entity test where test.a > :1", false)
    parse("entity test where a > 3", false)
    parse("""entity test where a > ":1"""", false)
    
    parse("""entity test where a > :1 and b == :2""", true)
    parse("""entity test where a > :1 and b == :1""", true)
    parse("""entity test where a > :1 and b == 1""", false)
    
    parse("""entity test where a > :1 and (b == :2 or b == :3)""", true)
    parse("""entity test where a > :1 and (b == :2 or (b == :3 or c != :4))""", true)
  }
  
  private def parse(query: String, success: Boolean) {
    var result = parser.parseAll(parser.query, query);
    assertEquals(success, result.successful)
  }
  

}
