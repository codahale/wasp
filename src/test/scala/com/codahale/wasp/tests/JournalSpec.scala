package com.codahale.wasp.tests

import com.codahale.wasp.Journal
import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.SortedMap
import com.codahale.simplespec.{ignore, BeforeAndAfterEach, Spec}

class JournalSpec extends Spec {
  class `Adding records to a journal` extends JournalContext {
    def `reads them back when the journal is re-opened` = {
      journal.add(1, "one")
      journal.add(2, "two")

      journal.stop()

      journal.start() must beEqualTo(SortedMap(1L -> "one", 2L -> "two"))
    }
  }

  class `Adding records to a journal and then removing them` extends JournalContext {
    def `doesn't read them back when the journal is re-opened` = {
      journal.add(1, "one")
      journal.add(2, "two")

      journal.remove(2)
      journal.remove(1)

      journal.stop()

      journal.start() must beEqualTo(SortedMap.empty[Long, String])
    }
  }

  class `Adding records to a journal and then updating them` extends JournalContext {
    def `reads the updated versions when the journal is re-opened` = {
      journal.add(1, "one")
      journal.add(2, "two")

      journal.update(2, "TWO")
      journal.update(1, "ONE")

      journal.stop()

      journal.start() must beEqualTo(SortedMap(1L -> "ONE", 2L -> "TWO"))
    }
  }
}

object JournalContext {
  private val count = new AtomicInteger()
}

trait JournalContext extends BeforeAndAfterEach {

  import JournalContext._

  private val tempDir = mkTempDir()
  private val _journal = newJournal()

  @ignore
  protected def journal = _journal

  @ignore
  protected def mkTempDir() = {
    val dir = new File(System.getProperty("java.io.tmpdir"), "journal-" + count.incrementAndGet + "/")
    dir.mkdirs()
    dir
  }

  @ignore
  protected def newJournal() =
    new Journal[String](tempDir, "temp.journal", StringCodec)

  override def beforeEach() {
    journal.start()
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
}
