package de.hpi.ads.database.types

case class TableSchema(columns: IndexedSeq[ColumnType]) {
    /**
      * Position of primary key.
      */
    val primaryKeyPosition: Int = 0

    def primaryKeyColumn: ColumnType = columns(primaryKeyPosition)

    def numValues: Int = columns.length

    def keyColumn: String = columns(primaryKeyPosition).name

    def columnNames: IndexedSeq[String] = columns.map(_.name)

    def columnPosition(columnName: String): Int = columnNames.indexOf(columnName)

    def columnPositions(columnNames: List[String]): List[Int] = columnNames.map(columnPosition)

    def rowSize: Int = columns.map(_.size).sum

    def rowSizeWithHeader: Int = this.rowSize + 1

    def columnsWithIndex: IndexedSeq[(ColumnType, Int)] = this.columns.zipWithIndex
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
