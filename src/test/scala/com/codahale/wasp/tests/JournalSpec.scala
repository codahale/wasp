package com.codahale.wasp.tests

import java.io.File
import scala.collection.immutable.SortedMap
import com.codahale.simplespec.Spec
import org.junit.Test
import com.codahale.wasp.{StringCodec, Journal}

class JournalSpec extends Spec {
  val tempDir = mkTempDir()
  val _journal = newJournal()

  def journal = _journal

  def mkTempDir() = {
    val dir = new File(System.getProperty("java.io.tmpdir"), "journal-" + System.nanoTime() + "/")
    dir.mkdirs()
    dir
  }

  def newJournal() =
    new Journal[String](tempDir, "temp.journal", StringCodec)

  override def beforeEach() {
    journal.start(new MapHandler[String])
  }

  override def afterEach() {
    journal.stop()

    def delete(f: File) {
      if (f != null) {
        if (f.isDirectory) {
          f.listFiles().map(delete)
        }
        f.delete()
      }
    }
    delete(tempDir)
  }

  class `Adding records to a journal` {
    @Test def `reads them back when the journal is re-opened` = {
      journal.add(1, "one")
      journal.add(2, "two")

      journal.stop()

      journal.start(new MapHandler[String]).must(be(SortedMap(1L -> "one", 2L -> "two")))
    }
  }

  class `Adding records to a journal and then removing them` {
    @Test def `doesn't read them back when the journal is re-opened` = {
      journal.add(1, "one")
      journal.add(2, "two")

      journal.remove(2)
      journal.remove(1)

      journal.stop()

      journal.start(new MapHandler[String]).must(be(SortedMap.empty[Long, String]))
    }
  }

  class `Adding records to a journal and then updating them` {
    @Test def `reads the updated versions when the journal is re-opened` = {
      journal.add(1, "one")
      journal.add(2, "two")

      journal.update(2, "TWO")
      journal.update(1, "ONE")

      journal.stop()

      journal.start(new MapHandler[String]).must(be(SortedMap(1L -> "ONE", 2L -> "TWO")))
    }
  }
}
