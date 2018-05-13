package de.hpi.ads.database.types

case class TableSchema(columns: List[ColumnType]) {
    /**
      * Position of primary key.
      */
    val primaryKeyPosition = 0

    def numValues: Int = columns.length

    def keyColumn: String = columns(primaryKeyPosition).name

    def columnNames: List[String] = columns.map(_.name)

    def columnIndex(columnName: String): Int = columnNames.indexOf(columnName)

    def columnIndices(columnNames: List[String]): List[Int] = columnNames.map(columnIndex)

    def columnPosition(columnName: String): Int = columnIndex(columnName)

    def columnPositions(relevantColumnNames: List[String]) : List[Int] = columnIndices(relevantColumnNames)

    def entrySize: Int = 0 // TODO
}

object TableSchema {
    def apply(schema: String) = new TableSchema(parseSchema(schema))

    def parseSchema(schema: String): List[ColumnType] = {
        // TODO parse schema string (column names, data types, column sizes)
        schema
            .split(";")
            .toList
            .map(name => ColumnType(name, StringType))
    }
}
