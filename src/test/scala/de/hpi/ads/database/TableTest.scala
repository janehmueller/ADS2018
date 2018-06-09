package de.hpi.ads.database

import de.hpi.ads.database.types._
import org.scalatest.{FlatSpec, Matchers}

class TableTest extends FlatSpec with Matchers {
    val tableFileName = "tableTest.ads"
    val tableFileFullPath: String = s"src/test/resources/$tableFileName"

    "Table" should "insert and read correctly" in {
        val schema = TableSchema(IndexedSeq(
            ColumnType("id", IntType),
            ColumnType("title", StringType()),
            ColumnType("long", LongType),
            ColumnType("double", DoubleType)))
        val row1 = List(1, "abcde", 34927L, 5.8)
        val row2 = List(2, "fgr", 344366927L, 4.9)
        val table = Table(tableFileFullPath, schema)
        table.insertList(row1)
        table.insertList(row2)
        val readRow1 = table.select(1).map(Row.fromBytes(_, schema))
        val readRow2 = table.select(2).map(Row.fromBytes(_, schema))
        table.cleanUp()
        readRow1 should contain (row1)
        readRow2 should contain (row2)
    }

    "Table splitting" should "split along the median" in {
        val schema = TableSchema("id:int;title:string(255)")
        val table = Table(tableFileFullPath, schema)
        val rows = List(
            List(0, "a"),
            List(1, "b"),
            List(2, "c"),
            List(3, "d"),
            List(4, "e"),
            List(5, "f"),
            List(6, "g"),
            List(7, "h"),
            List(8, "i"),
            List(9, "j"),
            List(10, "k")
        )
        rows.foreach(table.insertList)
        val (leftHalf, rightHalf, median) = table.readFileHalves
        table.cleanUp()
        median shouldEqual 5
        leftHalf should have length schema.rowSizeWithHeader * 5
        rightHalf should have length schema.rowSizeWithHeader * 6

        val leftRows = leftHalf
            .grouped(schema.rowSizeWithHeader)
            .map(row => row.slice(1, row.length))
            .map(Row.fromBytes(_, schema))
            .toList
        leftRows shouldEqual rows.slice(0, 5)

        val rightRows = rightHalf
            .grouped(schema.rowSizeWithHeader)
            .map(row => row.slice(1, row.length))
            .map(Row.fromBytes(_, schema))
            .toList
        rightRows shouldEqual rows.slice(5, rows.length)
    }

    it should "skip deleted rows" in {
        val schema = TableSchema("id:int;title:string(255)")
        val table = Table(tableFileFullPath, schema)
        val rows = List(
            List(0, "a"),
            List(1, "b"),
            List(2, "c"),
            List(3, "d"),
            List(4, "e"),
            List(5, "f"),
            List(6, "g"),
            List(7, "h"),
            List(8, "i"),
            List(9, "j"),
            List(10, "k")
        )
        rows.foreach(table.insertList)
        table.delete(10)
        val (leftHalf, rightHalf, median) = table.readFileHalves
        table.cleanUp()
        median shouldEqual 3
        leftHalf should have length schema.rowSizeWithHeader * 3
        rightHalf should have length schema.rowSizeWithHeader * 7

        val leftRows = leftHalf
            .grouped(schema.rowSizeWithHeader)
            .map(row => row.slice(1, row.length))
            .map(Row.fromBytes(_, schema))
            .toList
        leftRows shouldEqual rows.slice(0, 3)

        val rightRows = rightHalf
            .grouped(schema.rowSizeWithHeader)
            .map(row => row.slice(1, row.length))
            .map(Row.fromBytes(_, schema))
            .toList
        rightRows shouldEqual rows.slice(3, rows.length - 1)
    }
}
