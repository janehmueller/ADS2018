package de.hpi.ads.database.types

import scala.collection.mutable.{Map => MMap}

case class TableSchema(columns: IndexedSeq[ColumnType]) {
    /**
      * Position of primary key.
      */
    val primaryKeyPosition: Int = 0

    val columnNames: IndexedSeq[String] = columns.map(_.name)

    val columnOffsets: Map[String, Int] = this.calculateColumnOffsets

    def primaryKeyColumn: ColumnType = columns(primaryKeyPosition)

    def numValues: Int = columns.length

    def keyColumn: String = columns(primaryKeyPosition).name

    def columnPosition(columnName: String): Int = columnNames.indexOf(columnName)

    def columnPositions(columnNames: List[String]): List[Int] = columnNames.map(columnPosition)

    def columnByName(columnName: String): ColumnType = columns(columnPosition(columnName))

    def rowSize: Int = columns.map(_.size).sum

    def rowSizeWithHeader: Int = this.rowSize + 1

    def columnsWithIndex: IndexedSeq[(ColumnType, Int)] = this.columns.zipWithIndex

    def numColumns: Int = this.columns.length

    def calculateColumnOffsets: Map[String, Int] = {
        var index = 0
        var offset = 0
        val offsets = MMap.empty[String, Int]
        while (index < this.columns.length) {
            val column = this.columns(index)
            offsets(column.name) = offset
            offset += column.size
            index += 1
        }
        offsets.toMap
    }
}

object TableSchema {
    def apply(schema: String) = new TableSchema(parseSchema(schema))

    def parseSchema(schema: String): IndexedSeq[ColumnType] = {
        schema
            .split(";")
            .map { field =>
                val Array(name, dataType) = field.trim.split(":")
                ColumnType(name.trim, dataType.trim)
            }.toIndexedSeq
    }
}
