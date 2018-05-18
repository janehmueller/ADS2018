package de.hpi.ads.database

import de.hpi.ads.database.types._
import org.scalatest.{FlatSpec, Matchers}

class TableTest extends FlatSpec with Matchers {
    val tableFileName = "tableTest.ads"
    val tableFileFullPath: String = s"src/test/resources/$tableFileName"

    "Table" should "insert and read correctly" in {
        val schema = TableSchema(List(
            ColumnType("id", IntType),
            ColumnType("title", StringType),
            ColumnType("long", LongType),
            ColumnType("double", DoubleType)))
        val row1 = List(1, "abcde", 34927L, 5.8)
        val row2 = List(2, "fgr", 344366927L, 4.9)
        val table = Table(tableFileName, schema)
        table.insertList(row1)
        table.insertList(row2)
        val readRow1 = table.select(1).map(_.toList)
        val readRow2 = table.select(2).map(_.toList)
        table.cleanUp()
        readRow1 should contain (row1)
        readRow2 should contain (row2)
    }
}
