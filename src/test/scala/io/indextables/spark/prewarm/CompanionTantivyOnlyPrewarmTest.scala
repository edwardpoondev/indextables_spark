/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.indextables.spark.prewarm

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.SparkSession

import io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory

/**
 * Tests that PREWARM INDEXTABLES CACHE works correctly for companion tables when only
 * tantivy components (TERM_DICT, POSTINGS) are requested — no parquet segments.
 *
 * This reproduces the production bug where prewarm found 229 splits but warmed 0 because
 * `createSplitSearcherWithCompanionSupport` unconditionally used the 4-arg overload
 * (with parquetTableRoot), triggering Rust-side parquet initialization that failed when
 * Iceberg table credentials were unavailable on the query JVM.
 *
 * The fix makes the method context-aware: it only uses the 4-arg path when parquet
 * segments or FASTFIELD are requested. For tantivy-only prewarm (TERM, POSTINGS), it
 * uses the simpler 2-arg path that doesn't require Iceberg table access.
 *
 * No cloud credentials needed — runs entirely on local filesystem.
 */
class CompanionTantivyOnlyPrewarmTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with io.indextables.spark.testutils.FileCleanupHelper {

  private val logger = LoggerFactory.getLogger(classOf[CompanionTantivyOnlyPrewarmTest])

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("CompanionTantivyOnlyPrewarmTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(
        "spark.sql.extensions",
        "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
          "io.delta.sql.DeltaSparkSessionExtension"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.indextables.cache.disk.enabled", "false")
      .config("spark.indextables.aws.accessKey", "test-default-access-key")
      .config("spark.indextables.aws.secretKey", "test-default-secret-key")
      .config("spark.indextables.aws.sessionToken", "test-default-session-token")
      .config("spark.indextables.s3.pathStyleAccess", "true")
      .config("spark.indextables.aws.region", "us-east-1")
      .config("spark.indextables.s3.endpoint", "http://localhost:10101")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    _root_.io.indextables.spark.storage.SplitConversionThrottle.initialize(
      maxParallelism = Runtime.getRuntime.availableProcessors() max 1
    )
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("companion-tantivy-prewarm-test").toString
    try {
      flushCaches()
      f(path)
    } finally
      deleteRecursively(new File(path))
  }

  private def flushCaches(): Unit =
    try {
      GlobalSplitCacheManager.flushAllCaches()
      DriverSplitLocalityManager.clear()
    } catch {
      case _: Exception =>
    }

  private def createLocalDeltaTable(deltaPath: String, numRows: Int = 50): Unit = {
    val ss = spark
    import ss.implicits._

    val data = (0 until numRows).map(i => (i.toLong, s"name_$i", i * 2.5, s"category_${i % 5}", i % 100))
    data
      .toDF("id", "name", "score", "category", "priority")
      .repartition(1)
      .write
      .format("delta")
      .save(deltaPath)
  }

  private def buildCompanion(deltaPath: String, indexPath: String): Int = {
    val buildResult = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    )
    val buildRow = buildResult.collect()(0)
    buildRow.getString(buildRow.fieldIndex("status")) shouldBe "success"
    val splitsCreated = buildRow.getInt(buildRow.fieldIndex("splits_created"))
    splitsCreated should be > 0
    logger.info(s"Companion index built: $splitsCreated splits")
    splitsCreated
  }

  // ═══════════════════════════════════════════
  //  Tantivy-Only Prewarm Tests (the fix)
  // ═══════════════════════════════════════════

  test("TERM_DICT + POSTINGS prewarm should succeed for companion table") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_table").getAbsolutePath
      val indexPath = new File(tempDir, "companion_index").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 50)
      val splitsCreated = buildCompanion(deltaPath, indexPath)

      flushCaches()

      // This is the exact SQL our CompanionCacheWarmer runs in production.
      // Before the fix, this returned 0 prewarmed splits for companion tables.
      val prewarmResult = spark.sql(
        s"PREWARM INDEXTABLES CACHE '$indexPath' FOR SEGMENTS (TERM_DICT, POSTINGS)"
      )
      val prewarmRows = prewarmResult.collect()
      prewarmRows.length should be > 0

      val prewarmStatus   = prewarmRows.head.getAs[String]("status")
      val splitsPrewarmed = prewarmRows.map(_.getAs[Int]("splits_prewarmed")).sum

      logger.info(s"TERM_DICT + POSTINGS prewarm: status=$prewarmStatus, splits=$splitsPrewarmed/$splitsCreated")

      prewarmStatus shouldBe "success"
      prewarmStatus should not include "error"
      prewarmStatus should not include "failed preparation"
      splitsPrewarmed shouldBe splitsCreated
    }
  }

  test("default segments prewarm should succeed for companion table") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_table").getAbsolutePath
      val indexPath = new File(tempDir, "companion_index").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 30)
      val splitsCreated = buildCompanion(deltaPath, indexPath)

      flushCaches()

      // No FOR SEGMENTS clause — defaults to TERM + POSTINGS
      val prewarmResult = spark.sql(
        s"PREWARM INDEXTABLES CACHE '$indexPath'"
      )
      val prewarmRows = prewarmResult.collect()
      prewarmRows.length should be > 0

      val prewarmStatus   = prewarmRows.head.getAs[String]("status")
      val splitsPrewarmed = prewarmRows.map(_.getAs[Int]("splits_prewarmed")).sum

      logger.info(s"Default segments prewarm: status=$prewarmStatus, splits=$splitsPrewarmed/$splitsCreated")

      prewarmStatus shouldBe "success"
      splitsPrewarmed shouldBe splitsCreated
    }
  }

  test("TERM_DICT + POSTINGS prewarm followed by read should return correct data") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_table").getAbsolutePath
      val indexPath = new File(tempDir, "companion_index").getAbsolutePath
      val numRows   = 50

      createLocalDeltaTable(deltaPath, numRows)
      buildCompanion(deltaPath, indexPath)

      flushCaches()

      val prewarmResult = spark.sql(
        s"PREWARM INDEXTABLES CACHE '$indexPath' FOR SEGMENTS (TERM_DICT, POSTINGS)"
      )
      prewarmResult.collect().head.getAs[String]("status") shouldBe "success"

      // Read back through companion index and verify data
      val companionDf = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      val rows = companionDf.select("id", "name").collect()
      rows.length shouldBe numRows

      val actualIds = rows.map(_.getLong(0)).sorted
      actualIds shouldBe (0L until numRows).toArray

      logger.info(s"Read $numRows rows correctly after TERM_DICT + POSTINGS prewarm")
    }
  }

  // ═══════════════════════════════════════════
  //  Parquet segments still use 4-arg path
  // ═══════════════════════════════════════════

  test("PARQUET_COLUMNS prewarm should still succeed for companion table") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_table").getAbsolutePath
      val indexPath = new File(tempDir, "companion_index").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 30)
      buildCompanion(deltaPath, indexPath)

      flushCaches()

      // PARQUET_COLUMNS needs the 4-arg path — should still work
      val prewarmResult = spark.sql(
        s"PREWARM INDEXTABLES CACHE '$indexPath' FOR SEGMENTS (PARQUET_COLUMNS) ON FIELDS (score)"
      )
      val prewarmRows = prewarmResult.collect()
      prewarmRows.length should be > 0

      val prewarmStatus = prewarmRows.head.getAs[String]("status")
      prewarmStatus shouldBe "success"
      prewarmStatus should not include "partial"

      logger.info(s"PARQUET_COLUMNS prewarm status: $prewarmStatus")
    }
  }

  test("mixed segments (TERM, POSTINGS, PARQUET_COLUMNS) prewarm should succeed for companion table") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_table").getAbsolutePath
      val indexPath = new File(tempDir, "companion_index").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 30)
      buildCompanion(deltaPath, indexPath)

      flushCaches()

      // Mixed: tantivy + parquet → uses 4-arg path
      val prewarmResult = spark.sql(
        s"PREWARM INDEXTABLES CACHE '$indexPath' FOR SEGMENTS (TERM, POSTINGS, PARQUET_COLUMNS) ON FIELDS (score, name)"
      )
      val prewarmRows = prewarmResult.collect()
      prewarmRows.length should be > 0

      val prewarmStatus   = prewarmRows.head.getAs[String]("status")
      val splitsPrewarmed = prewarmRows.map(_.getAs[Int]("splits_prewarmed")).sum

      prewarmStatus shouldBe "success"
      splitsPrewarmed should be > 0

      logger.info(s"Mixed segments prewarm: status=$prewarmStatus, splits=$splitsPrewarmed")
    }
  }

  // ═══════════════════════════════════════════
  //  Non-companion table edge case
  // ═══════════════════════════════════════════

  test("TERM_DICT + POSTINGS prewarm should succeed for non-companion table") {
    withTempPath { tempDir =>
      val tablePath = new File(tempDir, "regular_table").getAbsolutePath
      val ss        = spark
      import ss.implicits._

      (0 until 20)
        .map(i => (i.toLong, s"title_$i", i * 1.5))
        .toDF("id", "title", "score")
        .coalesce(1)
        .write
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.indexWriter.batchSize", "50")
        .mode("append")
        .save(tablePath)

      flushCaches()

      val prewarmResult = spark.sql(
        s"PREWARM INDEXTABLES CACHE '$tablePath' FOR SEGMENTS (TERM_DICT, POSTINGS)"
      )
      val prewarmRows = prewarmResult.collect()
      prewarmRows.length should be > 0

      val prewarmStatus   = prewarmRows.head.getAs[String]("status")
      val splitsPrewarmed = prewarmRows.map(_.getAs[Int]("splits_prewarmed")).sum

      prewarmStatus shouldBe "success"
      splitsPrewarmed should be > 0

      logger.info(s"Non-companion TERM_DICT + POSTINGS prewarm: status=$prewarmStatus, splits=$splitsPrewarmed")
    }
  }

  // ═══════════════════════════════════════════
  //  Observability: status reflects prep failures
  // ═══════════════════════════════════════════

  test("prewarm status should surface preparation failures instead of silent success") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_table").getAbsolutePath
      val indexPath = new File(tempDir, "companion_index").getAbsolutePath

      createLocalDeltaTable(deltaPath, numRows = 20)
      buildCompanion(deltaPath, indexPath)

      flushCaches()

      // A successful prewarm should report "success", not "error: all N splits failed preparation"
      val prewarmResult = spark.sql(
        s"PREWARM INDEXTABLES CACHE '$indexPath' FOR SEGMENTS (TERM_DICT, POSTINGS)"
      )
      val prewarmRows = prewarmResult.collect()

      prewarmRows.foreach { row =>
        val status = row.getAs[String]("status")
        status should not include "failed preparation"
        status should not startWith "error"
      }

      val totalPrewarmed = prewarmRows.map(_.getAs[Int]("splits_prewarmed")).sum
      totalPrewarmed should be > 0

      logger.info(s"All tasks succeeded without preparation failures, total prewarmed: $totalPrewarmed")
    }
  }
}
