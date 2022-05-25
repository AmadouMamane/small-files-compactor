package com.mydevs

import java.time.LocalDate

import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.Logger

import scala.annotation.tailrec

/**
  * Provides database or data related classes to handle data in/out.
  *
  * ==Overview==
  * This package contains common classes to handle some common functions
  * such as read/write data from/to Hive, Cassandra or external RDBMS.
  * This package handle also any sqoop related functions (import/export).
  */
package object database {

  trait TableUtils {

    val table: String
    val actualDatabaseName: String
    val actualTableComment: String
    val actualColumnComments: Map[String, String]
    val actualTableProperties: Map[String, String]
    val actualPartitionBy: Option[Seq[String]]
    val actualFormat: String
    val spark: SparkSession
    val logger: Logger

    /**
     * object that wraps checks of database and table presence. Useful to gather exception messages and take actions from them.
     *
     * @param databaseExists Flag indicating if database exists (true) or not (false)
     * @param tableExists    Flag indicating if table exists in the database (true) or not (false)
     */
    case class TableExistence(databaseExists: Boolean, tableExists: Boolean) {
      val exists: Boolean = databaseExists && tableExists

      def report(): String = if (exists) "OK" else if (databaseExists) s"Table $table not found in database $actualDatabaseName" else s"Database $actualDatabaseName not found"
    }

    /**
     * Function that return a TableExistence report describing if database and table exist.
     *
     * @return TableExistence report
     */
    def getTableExistence: TableExistence = if (spark.catalog.databaseExists(actualDatabaseName)) {
      if (spark.catalog.tableExists(s"$actualDatabaseName.$table")) {
        TableExistence(databaseExists = true, tableExists = true)
      }
      else TableExistence(databaseExists = true, tableExists = false)
    }
    else TableExistence(databaseExists = false, tableExists = false)

    def createTableQuery(dataset: Dataset[_]): String = {
      // Build create string portion for partitioned columns
      val partitionColumns: String = actualPartitionBy match {
        case None => ""
        case Some(cols) =>
          val colsExist: Map[String, Boolean] = cols.map(col => (col, dataset.columns.contains(col))).toMap
          if (colsExist.values.reduce(_ && _)) {
            s"PARTITIONED BY ${
              dataset.schema.fields
                .filter(f => cols.contains(f.name))
                .map(f => buildSqlColumnCreateString(f.name, f.dataType.catalogString))
                .mkString("(", ", ", ")")
            }"
          }
          else throw new Exception(s"Table creation with partition columns ${cols.mkString("'", " and ", "'")} cannot be performed. Columns not found in dataset: ${colsExist.filterNot { case (_, v) => v }.keySet.mkString(", ")}")
      }

      // Build create string portion for regular columns
      val regularColumns: String = dataset.schema.fields
        .filter(f => !actualPartitionBy.getOrElse(Seq.empty[String]).contains(f.name))
        .map(f => buildSqlColumnCreateString(f.name, f.dataType.catalogString))
        .mkString("(", ", ", ")")

      // Build the create table query string
      var query = s"CREATE TABLE $actualDatabaseName.$table $regularColumns"
      if (!partitionColumns.isEmpty)
        query = s"$query $partitionColumns"
      if (!actualTableComment.isEmpty)
        query = s"$query COMMENT '${escapeSqlComment(actualTableComment)}'"
      query = s"$query STORED AS $actualFormat"
      if (!sqlTablePropertiesString.isEmpty)
        query = s"$query $sqlTablePropertiesString"
      query
    }

    /**
     * Escape SQL comment string (simple quote escaped with a backslash)
     *
     * @param comment Comment to escape
     * @return Escaped comment
     */
    def escapeSqlComment(comment: String): String = comment.replaceAll("[']", "\\\\'")

    /**
     * Build column specification string for SQL create table statement.
     *
     * @param columnName Name of the column to build the string for
     * @param columnType Type of the column to build the string for
     * @return The query string for the column
     */
    def buildSqlColumnCreateString(columnName: String, columnType: String): String = {
      var comment: String = actualColumnComments.getOrElse(columnName, "")
      if (comment.isEmpty)
        s"$columnName $columnType"
      else
        s"$columnName $columnType COMMENT '${escapeSqlComment(comment)}'"
    }


    def sqlTablePropertiesString: String = {
      def doubleQuote(inputString: String) = raw""""$inputString""""

      actualTableProperties.isEmpty match {
        case false =>
          "TBLPROPERTIES (".concat(actualTableProperties.map(property => doubleQuote(property._1)
            .concat(s"=${doubleQuote(property._2)}")).mkString(",")).concat(")")
        case true => ""
      }
    }

    @tailrec
    final def createTableIfNotExists(dataset: Dataset[_]): Unit = {
      getTableExistence match {
        case TableExistence(true, true) =>
        case TableExistence(false, _) => throw new Exception(s"Database $actualDatabaseName not found")
        case TableExistence(true, false) =>
          logger.warn(s"The table $table does not exists in the database $actualDatabaseName, so it will be created.")
          spark.sql(createTableQuery(dataset))
          createTableIfNotExists(dataset)
      }
    }

  }

  case class CompactorInstance(instance: Long, toDate: LocalDate)
}
