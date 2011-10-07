package com.codahale.wasp

import scala.collection.JavaConversions._
import scala.collection.immutable.SortedMap
import java.io.File
import java.util.{List, ArrayList}
import org.slf4j.LoggerFactory
import org.hornetq.core.asyncio.impl.AsynchronousFileImpl
import org.hornetq.core.journal.impl.{JournalImpl, AIOSequentialFileFactory, NIOSequentialFileFactory}
import org.hornetq.core.journal.{TransactionFailureCallback, PreparedTransactionInfo, RecordInfo}

/**
  * An append-only transaction log.
  *
  * @param directory The directory where the journal files will be stored.
  * @param filenamePattern The pattern of journal filenames. ("journal.file" => "journal-1.file")
  * @param codec The {@link Codec} used to encode and decode values.
  * @param journalFileSize The maximum size, in bytes, of each journal file.
  * @param minimumJournalFileCount The initial number of journal files to create.
  * @param journalFileCountCompactionThreshold The minimum number of journal files before Wasp will perform a compaction.
  * @param fragmentationCompactionThreshold The minimum percentage (0-100) of fragmentation before Wasp will perform a compaction.
  * @param maxAIOSyncs The maximum number of write requests that can be in the AIO queue at any one time.
  *
  */
class Journal[A](directory: File,
                 filenamePattern: String,
                 codec: Codec[A],
                 journalFileSize: Int = 10 * 1024 * 1024, // 10MB
                 minimumJournalFileCount: Int = 2,
                 journalFileCountCompactionThreshold: Int = 10,
                 fragmentationCompactionThreshold: Int = 30,
                 maxAIOSyncs: Int = 500) {
  require(directory.isDirectory)

  private val logger = LoggerFactory.getLogger(getClass)
  private val fileFactory = if (AsynchronousFileImpl.isLoaded) {
    logger.info("Using AIO access for journal files")
    new AIOSequentialFileFactory(directory.getAbsolutePath)
  } else {
    if (System.getProperty("os.name").contains("Linux") && System.getProperty("os.version").startsWith("2.6")) {
      logger.warn("Unable to locate the HornetQAIO library, which is available for this platform.")
      logger.warn("AIO access can handle a much higher write load without compromising durability.")
      logger.warn("It is *highly* recommended that you use it.")
    }
    logger.info("Using NIO access for journal files")
    new NIOSequentialFileFactory(directory.getAbsolutePath)
  }

  private val journal = {
    val filenameComponents = filenamePattern.split("""\.""", 2)
    new JournalImpl(
      journalFileSize,
      minimumJournalFileCount,
      journalFileCountCompactionThreshold,
      fragmentationCompactionThreshold,
      fileFactory,
      filenameComponents(0),
      filenameComponents(1),
      maxAIOSyncs
    )
  }

  def start[B](handler: ReplayHandler[A, B]) = {
    logger.info("Starting journal")
    journal.start()

    val records = new ArrayList[RecordInfo]()
    val transactions = new ArrayList[PreparedTransactionInfo]()
    journal.load(records, transactions, new TransactionFailureCallback {
      def failedTransaction(transactionID: Long,
                            records: List[RecordInfo],
                            recordsToDelete: List[RecordInfo]) {
        logger.warn("Failed to recover txn %d (%d records, %d to be deleted)"
          .format(transactionID, records.size(), recordsToDelete.size()))
      }
    })

    logger.info("Loaded {} records from journal", records.size())
    logger.debug("Ignoring {} prepared transactions", transactions.size())

    val iterator = records.iterator()
    while (iterator.hasNext) {
      val record = iterator.next()
      handler.process(record.id, codec.decode(record.data))
    }
    handler.result()
  }

  def stop() {
    logger.info("Stopping journal")
    journal.stop()
  }

  def add(id: Long, value: A) {
    journal.appendAddRecord(id, codec.recordType, codec.encode(value), true)
  }

  def update(id: Long, value: A) {
    journal.appendUpdateRecord(id, codec.recordType, codec.encode(value), true)
  }

  def remove(id: Long) {
    journal.appendDeleteRecord(id, true)
  }
}
