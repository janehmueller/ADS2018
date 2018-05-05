package de.hpi.ads.database.types

class TableSchema(schemaString: String) {
    val columns: List[String] = schemaString.split(";").toList

    val numValues: Int = columns.length

    /**
      * Position of primary key.
      */
    val primaryKeyPosition = 0

    val key: String = columns(primaryKeyPosition)

    def columnPosition(columnName: String): Int = columns.indexOf(columnName)
}
