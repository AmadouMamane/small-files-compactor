package com.mydevs.database

import java.time.LocalDate
import java.util.concurrent.Executors

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success, Try}

/**
 * API for writing data into Hadoop according to HDFS block size
 *
 * ==Overview==
 * This includes
 *  - A method to write data into hive tables by producing the exact number of files expected (data volume divided by HDFS block size)
 *  - A faster method to write data into hive tables but it produces the approximate number of files expected
 *  - A method to write into HDFS directory by producing the exact number of files expected
 *
 *  This methods work also for a partitioned table
 */
object Compactor {

  def writeIntoHive(dsToWrite: Dataset[_], hiveTableName: String, databaseName: String, fileFormat: String = "orc", writeMode: String = "overwrite", spark: SparkSession)
                   (tableComment: Option[String] = None, columnComments: Option[Map[String, String]] = None, tableProperties: Option[Map[String, String]] = None, tablePartioningColumnsNames: Option[Seq[String]] = None, createHiveTableIfNotExists: Boolean = true, validateDateFrameSchemaAgainstTable: Boolean = false, checkPoint: Boolean = true): Unit =
    new Compactor(dsToWrite, hiveTableName, databaseName, fileFormat, writeMode, "", None, tableComment, columnComments, tableProperties, tablePartioningColumnsNames, createHiveTableIfNotExists, validateDateFrameSchemaAgainstTable, checkPoint, spark).writeIntoHive()

  def approxWriteIntoHive(dsToWrite: Dataset[_], hiveTableName: String, databaseName: String, fileFormat: String = "orc", writeMode: String = "overwrite", spark: SparkSession)
                         (tableComment: Option[String] = None, columnComments: Option[Map[String, String]] = None, tableProperties: Option[Map[String, String]] = None, tablePartioningColumnsNames: Option[Seq[String]] = None, createHiveTableIfNotExists: Boolean = true, fasterButLessPreciseCompaction: Boolean = false, evenFasterButLessPreciseCompaction: Boolean = true, validateDateFrameSchemaAgainstTable: Boolean = false, checkPoint: Boolean = true): Unit =
    new Compactor(dsToWrite, hiveTableName, databaseName, fileFormat, writeMode, "", None, tableComment, columnComments, tableProperties, tablePartioningColumnsNames, createHiveTableIfNotExists, validateDateFrameSchemaAgainstTable, checkPoint, spark).approxWriteIntoHive(fasterButLessPreciseCompaction, evenFasterButLessPreciseCompaction)

  def writeIntoDirectory(dsToWrite: Dataset[_], path: String, fileFormat: String = "orc", writeMode: String = "overwrite", options: Option[Map[String, String]] = None, spark: SparkSession)(evenFasterButLessPreciseCompaction: Boolean = false, checkPoint: Boolean = true): Unit =
    new Compactor(dsToWrite, "", "", fileFormat, writeMode, path, options, checkPoint = checkPoint, sparkS = spark).writeIntoDirectory(evenFasterButLessPreciseCompaction)

}


class Compactor(dsToWrite: Dataset[_],
                hiveTableName: String,
                databaseName: String,
                fileFormat: String = "orc",
                writeMode: String = "overwrite",
                path: String = "",
                options: Option[Map[String, String]] = None,
                tableComment: Option[String] = None,
                columnComments: Option[Map[String, String]] = None,
                tableProperties: Option[Map[String, String]] = None,
                tablePartioningColumnsNames: Option[Seq[String]] = None,
                createHiveTableIfNotExists: Boolean = true,
                validateDateFrameSchemaAgainstTable: Boolean = false,
                checkPoint: Boolean = true,
                sparkS: SparkSession) extends TableUtils {

  override val table: String = hiveTableName
  override val actualDatabaseName: String = databaseName
  override val actualTableComment: String = tableComment.getOrElse("")
  override val actualColumnComments: Map[String, String] = columnComments.getOrElse(Map.empty[String, String])
  override val actualTableProperties: Map[String, String] = tableProperties.getOrElse(Map.empty[String, String])
  override val actualPartitionBy: Option[Seq[String]] = tablePartioningColumnsNames
  override val actualFormat: String = fileFormat.toLowerCase match {
    case "csv" | "json" | "text" => "textfile"
    case other => other
  }
  override val spark = sparkS
  val LOGGER: Logger = LoggerFactory.getLogger("Compactor-Logger")
  override val logger = LOGGER

  val HDFS_BLOCK_SIZE = 1024 * 1024 * 256
  val HDFS = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val INSTANCE = CompactorInstance(Random.nextLong.abs, LocalDate.now)
  val BASE_TEMP_DIR = s"${HDFS.getHomeDirectory.toString}/compactor_tmp"
  val INSTANCE_DIR = s"$BASE_TEMP_DIR/${INSTANCE.toDate}/${INSTANCE.instance}"

  val startTime = System.currentTimeMillis()

  updateLoggingLevel("warn")

  import spark.implicits._

  spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")


  val dfToWrite = if (hiveTableName.nonEmpty && databaseName.nonEmpty) {

    spark.sparkContext.setLocalProperty("callSite.short", s"Compactor writing into the table $hiveTableName")

    spark.catalog.setCurrentDatabase(databaseName)

    createHiveTableIfNotExists match {
      case true => createTableIfNotExists(dsToWrite)
      case false =>
    }

    validateDateFrameSchemaAgainstTable match {
      case true =>
        val columnsNames = spark.sql(s"show columns in $databaseName.$hiveTableName").select("col_name").collect.map(r=>r.getString(0))
        val validatedDf = dsToWrite.selectExpr(columnsNames: _*)
        if (checkPoint) {
          spark.createDataFrame(validatedDf.rdd, validatedDf.schema).cache()
        } else {
          validatedDf
        }
      case _ =>
        if (checkPoint) {
          spark.createDataFrame(dsToWrite.toDF().rdd, dsToWrite.schema).cache()
        } else {
          dsToWrite
        }
    }

  } else {

    if (checkPoint) {
      spark.createDataFrame(dsToWrite.toDF().rdd, dsToWrite.schema).cache()
    } else {
      dsToWrite
    }

  }

  val totalNbrRows = dfToWrite.count


  def writeIntoHive(): Unit = {

    if (totalNbrRows == 0) {
      LOGGER.warn(s"Exiting because the provided dataframe to write into table $hiveTableName is empty")
      return
    }

    val existingTableFileFormat = getTableFormat

    require(existingTableFileFormat.isDefined && existingTableFileFormat.get == actualFormat, s"The provided filefileFormat $fileFormat for compaction does not match the destination table's fileFormat ${existingTableFileFormat.get}")

    val isAPartitionedTable = isItAPartitionedTable
    val hiveTablePartitioningKeys: Seq[String] = getTablePartitioningKeys(isAPartitionedTable)

    val sampledDfStatistics = getSampledDfStatistics
    val nbrOfFilesToWrite = resolveNbrOfFilesToWrite(totalNbrRows, sampledDfStatistics, hiveTableName)

    if (nbrOfFilesToWrite == 1) {
      LOGGER.warn(s"Writing with one executor into the table $hiveTableName")
      dfToWrite.coalesce(1).write.format(fileFormat).mode(writeMode).insertInto(hiveTableName)
    } else {

      if (!isAPartitionedTable) {
        LOGGER.warn(s"Writing $nbrOfFilesToWrite file(s) into the non partitioned table $hiveTableName")
        dfToWrite.repartition(nbrOfFilesToWrite).write.format(fileFormat).mode(writeMode).insertInto(hiveTableName)
      } else {

        val filterExpr = hiveTablePartitioningKeys.map(x => s"concat('$x=', concat('\\'', concat($x, '\\'')))").reduce((x, y) => s"concat(concat($x, ' and '), $y)")

        val partitionsToWrite = dfToWrite.selectExpr(filterExpr).distinct.as[String].collect

        val nbrPartitionsToWrite = partitionsToWrite.size

        val parallelismLevel = math.max(nbrOfFilesToWrite, nbrPartitionsToWrite)

        //spark.conf.set("spark.sql.shuffle.partitions", parallelismLevel)

        val concurrentThreads = math.min(40, parallelismLevel)

        if (nbrPartitionsToWrite == 1) {
          dfToWrite.repartition(nbrOfFilesToWrite).write.format(fileFormat).mode(writeMode).insertInto(hiveTableName)
        } else {

          implicit val ec = ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(concurrentThreads))

          val futures = partitionsToWrite.map(part => Future {
            val dfSinglePartitionToWrite = dfToWrite.filter(part)
            val nbrOfFilesToWriteForPartition = resolveNbrOfFilesToWrite(dfSinglePartitionToWrite.count, sampledDfStatistics, part)

            LOGGER.warn(s"Writing $nbrOfFilesToWriteForPartition file(s) into the partition $part of the table $hiveTableName")
            //LOGGER.warn(s"Writing ${dfSinglePartitionToWrite.count} into $part with ${sampledDfStatistics._1}  counSample and ${sampledDfStatistics._2} size")
            dfSinglePartitionToWrite.repartition(nbrOfFilesToWriteForPartition).write.format(fileFormat).mode(writeMode).insertInto(hiveTableName)
            LOGGER.warn(s"Finished Writing $part")
          })

          futures.foreach(f => Await.ready(f, Duration.Inf))

        }
      }
    }

    deleteTemporaryFiles()
    dfToWrite.unpersist()
    val endTime = System.currentTimeMillis()
    val duration = (endTime - startTime) / 60000.0
    val durationTruncated = f"$duration%.3f"
    LOGGER.warn(s"Finished writing files into table $hiveTableName, the loading operation took $durationTruncated minutes")
  }


  def approxWriteIntoHive(fasterButLessPreciseCompaction: Boolean = false, evenFasterButLessPreciseCompaction: Boolean = true): Unit = {

    if (totalNbrRows == 0) {
      LOGGER.warn(s"Exiting because the provided dataframe to write into table $hiveTableName is empty")
      return
    }

    val existingTableFileFormat = getTableFormat

    require(existingTableFileFormat.isDefined && existingTableFileFormat.get == actualFormat, s"The provided filefileFormat $fileFormat for compaction does not match the destination table's fileFormat ${existingTableFileFormat.get}")

    val isAPartitionedTable = isItAPartitionedTable

    val hiveTablePartitioningKeys: Seq[String] = getTablePartitioningKeys(isAPartitionedTable)

    val sampledDfStatistics = getSampledDfStatistics
    val nbrOfFilesToWrite = resolveNbrOfFilesToWrite(totalNbrRows, sampledDfStatistics, hiveTableName)

    if (nbrOfFilesToWrite == 1) {
      LOGGER.warn(s"Writing with one executor into the table $hiveTableName")
      dfToWrite.coalesce(1).write.format(fileFormat).mode(writeMode).insertInto(hiveTableName)
    } else {

      val dfNbrOfPartitions = dfToWrite.rdd.partitions.size

      if (!isAPartitionedTable) {

        LOGGER.warn(s"Writing $nbrOfFilesToWrite file(s) into the non partitioned table $hiveTableName")

        evenFasterButLessPreciseCompaction match {
          case true if dfNbrOfPartitions >= nbrOfFilesToWrite =>
            dfToWrite.coalesce(nbrOfFilesToWrite).write.format(fileFormat).mode(writeMode).insertInto(hiveTableName)
          case _ =>
            dfToWrite.repartition(nbrOfFilesToWrite).write.format(fileFormat).mode(writeMode).insertInto(hiveTableName)
        }

      } else {

        val nbrOfHivePartitionsToWrite = resolveNbrOfHivePartitionsToWrite(hiveTablePartitioningKeys)
        val repartitioningValue = Math.max(nbrOfFilesToWrite, nbrOfHivePartitionsToWrite)

        //spark.conf.set("spark.sql.shuffle.partitions", repartitioningValue)

        evenFasterButLessPreciseCompaction match {
          case true =>

            val maxFilesWriten = repartitioningValue * nbrOfHivePartitionsToWrite
            LOGGER.warn(s"Writing at most $maxFilesWriten file(s) into the table $hiveTableName with $repartitioningValue partitions")

            if (dfNbrOfPartitions >= repartitioningValue) {
              dfToWrite.coalesce(repartitioningValue).write.format(fileFormat).mode(writeMode).insertInto(hiveTableName)
            } else {
              dfToWrite.repartition(repartitioningValue).write.format(fileFormat).mode(writeMode).insertInto(hiveTableName)
            }

          case _ if fasterButLessPreciseCompaction =>
            val maxFilesWriten = repartitioningValue * nbrOfHivePartitionsToWrite
            LOGGER.warn(s"Writing at most $maxFilesWriten file(s) into the table $hiveTableName with $repartitioningValue partitions")
            dfToWrite.repartition(repartitioningValue).write.format(fileFormat).mode(writeMode).insertInto(hiveTableName)

          case _ =>
            val maxFilesWriten = repartitioningValue + nbrOfHivePartitionsToWrite
            LOGGER.warn(s"Writing at most $maxFilesWriten file(s) into the table $hiveTableName with $repartitioningValue partitions")
            dfToWrite.repartitionByRange(repartitioningValue, hiveTablePartitioningKeys.map(col(_)) ++ Seq(rand()): _*).write.format(fileFormat).mode(writeMode).insertInto(hiveTableName)
        }
      }
    }

    deleteTemporaryFiles()
    dfToWrite.unpersist()
    val endTime = System.currentTimeMillis()
    val duration = (endTime - startTime) / 60000.0
    val durationTruncated = f"$duration%.3f"
    LOGGER.warn(s"Finished writing files into table $hiveTableName, the loading operation took $durationTruncated minutes")
  }


  def writeIntoDirectory(evenFasterButLessPreciseCompaction: Boolean = false): Unit = {

    spark.sparkContext.setLocalProperty("callSite.short", s"Compactor writing into directory $path")

    if (totalNbrRows == 0) {
      LOGGER.warn(s"Exiting because the provided dataframe to write into directory $path is empty")
      return
    }

    val nbrOfFilesToWrite = resolveNbrOfFilesToWrite(totalNbrRows, getSampledDfStatistics, path)
    val dfNbrOfPartitions = dfToWrite.rdd.partitions.size

    if (nbrOfFilesToWrite == 1) {
      LOGGER.warn(s"Writing $nbrOfFilesToWrite file(s) into $path")
      if (options.isDefined) dfToWrite.coalesce(1).write.options(options.get).format(fileFormat).mode(writeMode).save(path)
      else dfToWrite.coalesce(1).write.format(fileFormat).mode(writeMode).save(path)
    } else {
      LOGGER.warn(s"Writing $nbrOfFilesToWrite file(s) into $path")
      if (evenFasterButLessPreciseCompaction && (nbrOfFilesToWrite <= dfNbrOfPartitions)) {
        if (options.isDefined) dfToWrite.coalesce(nbrOfFilesToWrite).write.options(options.get).format(fileFormat).mode(writeMode).save(path)
        else dfToWrite.coalesce(nbrOfFilesToWrite).write.format(fileFormat).mode(writeMode).save(path)
      } else {
        if (options.isDefined) dfToWrite.repartition(nbrOfFilesToWrite).write.options(options.get).format(fileFormat).mode(writeMode).save(path)
        else dfToWrite.repartition(nbrOfFilesToWrite).write.format(fileFormat).mode(writeMode).save(path)
      }
    }

    deleteTemporaryFiles()
    dfToWrite.unpersist()
    val endTime = System.currentTimeMillis()
    val duration = (endTime - startTime) / 60000.0
    val durationTruncated = f"$duration%.3f"
    LOGGER.warn(s"Finished writing $nbrOfFilesToWrite file(s) into $path, the loading operation took $durationTruncated minutes")
  }


  /**
   * Sample the input dataframe and produce a row size
   *
   * @return Number of rows inside the sample and the size in bytes of the sample.
   *         Notice that this is the estimated size of a row
   *
   */
  private def getSampledDfStatistics = {
    val uid: String = System.currentTimeMillis() + "_" + Random.nextInt.abs
    val samplingPath = INSTANCE_DIR + "/" + uid
    val nbrRowsEstimator = Math.min(Math.ceil(totalNbrRows * 5d / 100).toInt, 10000)
    //LOGGER.warn(s"The computed approximate number of rows to get after sampling the dataframe is: $nbrRowsEstimator")
    val rawSampleDf = dfToWrite.sample(false, nbrRowsEstimator * 1.0 / totalNbrRows).cache
    val sampleDf = if (rawSampleDf.count == 0) dfToWrite.limit(1).cache else rawSampleDf
    val countSample = sampleDf.count
    //LOGGER.warn(s"Estimating dataframe size with ${sampleDf.count} rows")
    sampleDf.coalesce(1).write.format(fileFormat).mode(writeMode).save(samplingPath)
    sampleDf.unpersist()
    rawSampleDf.unpersist()
    (countSample, getFileSizeByPath(samplingPath).toDouble)
  }

  /**
   * @param sampleStats the stats produced by the function getSampledDfStatistics
   * @return the raw number of files to write regarding the input dataframe size
   */
  private def resolveNbrOfFilesToWrite(totalNbrRows: Long, sampleStats: (Long, Double), entity: String): Int = {
    val computedNbr = sampleStats._2 * totalNbrRows * 1.0 / (sampleStats._1 * HDFS_BLOCK_SIZE)
    val rawNbr = Math.ceil(computedNbr).toInt
    //An error factor different of 1 is computed when the computed number of files to write
    //is close to it's rounded value by a scale of 0.2
    //val errorFactor = if ((rawNbr - computedNbr) <= 0.2) rawNbr / computedNbr else 1
    //val nbr = Math.ceil(rawNbr * errorFactor).toInt
    val nbr = rawNbr match {
      case small if small < 10 => if ((small - computedNbr) <= 0.3) small + 1 else small
      case medium if medium < 20 => medium + 1
      case largerMedium if largerMedium < 50 => Math.ceil(largerMedium * 1.04).toInt
      case large => Math.ceil(large * 1.05).toInt
    }
    LOGGER.warn(s"[$entity] - the estimated number of files to write is: $nbr, and the raw computed was: $computedNbr")
    nbr
  }

  private def getFileSizeByPath(inputPath: String): Long = HDFS.getContentSummary(new Path(inputPath)).getLength

  private def deleteTemporaryFiles() = {
    //Deleting this instance data
    if (HDFS.exists(new Path(INSTANCE_DIR))) HDFS.delete(new Path(INSTANCE_DIR), true)
    LOGGER.warn(s"Deleted directory $INSTANCE_DIR")
    //Deleting 10 days ago data if any
    val oldDataPathToDelete = s"$BASE_TEMP_DIR/${INSTANCE.toDate.minusDays(10)}"
    if (HDFS.exists(new Path(oldDataPathToDelete))) {
      HDFS.delete(new Path(oldDataPathToDelete), true)
      LOGGER.warn(s"Deleted directory $oldDataPathToDelete")
    }
  }

  private def resolveNbrOfHivePartitionsToWrite(hiveTablePartioningKeys: Seq[String]): Int =
    dfToWrite.selectExpr(hiveTablePartioningKeys: _*).distinct.count.toInt

  private def updateLoggingLevel(level: String = "info") = spark.sparkContext.setLogLevel(level)

  private def getTableFormat = {
    if (spark.catalog.tableExists(databaseName, hiveTableName)) {
      val rawFormat = spark.sql(s"desc formatted $databaseName.$hiveTableName").filter('col_name === "InputFormat").select("data_type").as[String].head
      val fileFormat = rawFormat match {
        case "org.apache.hadoop.mapred.TextInputFormat" => "textfile"
        case "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat" => "orc"
        case "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" => "avro"
        case "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" => "parquet"
        case "org.apache.hadoop.mapred.SequenceFileInputFormat" => "sequencefile"
        case s => throw new Exception(s"Unknown table file fileFormat: $s")
      }
      Some(fileFormat)
    } else None
  }

  private def isItAPartitionedTable = Try(spark.sql(s"show partitions $hiveTableName")) match {
    case Success(_) => true
    case Failure(_) => false
  }

  private def getTablePartitioningKeys(isAPartitionedTable: Boolean): Seq[String] = isAPartitionedTable match {
    case true => {
      Try(spark.sql(s"show partitions $hiveTableName").as[String].first.split("/").map(_.split("=")(0))) match {
        case Success(s) => s.toSeq
        case Failure(_) => spark.sql(s"show create table $hiveTableName").select(regexp_extract('createtab_stmt, ".*PARTITIONED BY \\((.*)\\)", 1)).as[String].first.split(",").map(_.trim.split(" ")(0)).map(_.replaceAll("`", ""))
      }
    }
    case _ => Nil
  }

}

