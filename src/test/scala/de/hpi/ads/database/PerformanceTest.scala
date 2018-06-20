package de.hpi.ads.database

import java.nio.file.{Files, Paths}

import de.hpi.ads.database.operators.{EqOperator, LessThanOperator}
import de.hpi.ads.database.types._
import org.scalatest.{FlatSpec, Matchers}

class PerformanceTest extends FlatSpec with Matchers {
    val tableFileName = "tableTest.ads"
    val tableFileFullPath: String = s"src/test/resources/$tableFileName"

    "Table" should "insert and read correctly" in {
        val schema = TableSchema("id:int;title:string(20)")
        val row = List(1, "Great Movie")
        val table = Table(tableFileFullPath, schema)

        val msgCount = 10000000
        val tInsertStart = System.nanoTime()
        table.insertList(row)
        for (i <- 2 to msgCount) {
            table.insertList(List(i, "Some Other Movie"))
        }
        val tInsertEnd = System.nanoTime()
        println(s"Elapsed time (Inserting): ${(tInsertEnd - tInsertStart)/1000000000.0}s")

        println(s"File size: ${Files.size(Paths.get(tableFileFullPath))}")
        println(s"Table size: ${table.length}")
        val tFileStart = System.nanoTime()
        Files.readAllBytes(Paths.get(tableFileFullPath))
        val tFileEnd = System.nanoTime()
        println(s"Elapsed time (File read): ${(tFileEnd - tFileStart)/1000000000.0}s")

        var result = table.selectWhere(EqOperator("id", 1)).map(Row.fromBytes(_, schema))
        val tSelectEnd = System.nanoTime()
        println(s"Elapsed time (Reading EqOperator): ${(tSelectEnd - tFileEnd)/1000000000.0}s")
        result should have length 1
        result shouldEqual List(row)

        result = table.selectWhere(LessThanOperator("id", 10)).map(Row.fromBytes(_, schema))
        val tSelectMultipleEnd = System.nanoTime()
        println(s"Elapsed time (Reading LessThanOperator): ${(tSelectMultipleEnd - tSelectEnd)/1000000000.0}s")
        result should have length 9

//        table.cleanUp()
    }
}
