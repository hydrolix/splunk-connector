package io.hydrolix.connector.splunk

import org.junit.{Ignore, Test}

import io.hydrolix.connectors.types.ValueType

class ValueTypeParsingTest {
  @Ignore("doesn't work, don't know why")
  @Test
  def FAFO(): Unit = {
    println(ValueType.parse("""struct<"timestamp":timestamp(3),"level":string,"message":string>"""))
  }
}
