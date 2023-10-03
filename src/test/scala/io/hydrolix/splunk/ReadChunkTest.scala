package io.hydrolix.splunk

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.time.Instant

import com.google.common.io.ByteStreams
import org.junit.Assert.{assertArrayEquals, assertEquals, fail}
import org.junit.Test

import io.hydrolix.connectors.JSON

class ReadChunkTest {
  private def message(meta: Array[Byte], data: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream(meta.length + data.length + 128)
    writeChunk(baos, meta, Some(data.length, new ByteArrayInputStream(data)))
    baos.toByteArray
  }

  private val getinfo = ChunkedRequestMetadata(
    "getinfo",
    false,
    false,
    false,
    SearchInfo(
      List("hello"),
      List("hello"),
      "/dev/null",
      "12345",
      "hydrolix",
      "admin",
      "admin",
      "skey",
      "http://localhost:8000",
      "1.2.3",
      "|foo",
      "hdxquery",
      -1,
      Instant.EPOCH.getEpochSecond.toInt,
      Instant.EPOCH.getEpochSecond.toInt
    )
  )
  private val getInfoBytes = JSON.objectMapper.writeValueAsBytes(getinfo)

  private val helloWorld = "hello\tworld\n".getBytes("UTF-8")

  @Test
  def getInfoWithData(): Unit = {
    val minimal = message(getInfoBytes, helloWorld)

    readChunk(new ByteArrayInputStream(minimal)) match {
      case (meta, Some((len, stream))) =>
        assertEquals(getinfo, meta)
        assertEquals(len, helloWorld.length)
        assertArrayEquals(helloWorld, ByteStreams.toByteArray(stream))
      case other =>
        fail(other.toString())
    }
  }

  @Test
  def getInfoNoData(): Unit = {
    val minimal = message(getInfoBytes, Array())

    readChunk(new ByteArrayInputStream(minimal)) match {
      case (meta, None) =>
        assertEquals(getinfo, meta)
      case other =>
        fail(other.toString())
    }
  }
}
