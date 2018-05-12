package de.hpi.ads.database.types

import scala.collection.mutable.ListBuffer

case class TableSchema(columnNames: List[String], columnDataTypes: List[Any], columnSizes: List[Int]) {

    val numberOfColumns: Int = columnDataTypes.length

    val primaryKeyPosition = 0

    val keyColumnName: String = columnNames(primaryKeyPosition)

    val entrySize = columnSizes.foldLeft(0)(_ + _)

    def columnPosition(columnName: String): Int = columnNames.indexOf(columnName)

    def columnPositions(relevantColumnNames: List[String]) : List[Int] = {
        val result: ListBuffer[Int] = ListBuffer()
        val it1 = relevantColumnNames.iterator
        val it2 = columnNames.iterator
        var j = 0
        var colName: String = it1.next()
        while (it2.hasNext) {
            if (colName == it2.next()) {
                result += j
                j += 1
                colName = it1.next()
            }
            it2.next()
        }
        result.toList
    }
}
