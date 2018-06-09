package de.hpi.ads.database

import de.hpi.ads.database.types.TableSchema

import scala.collection.mutable.{IndexedSeq => MIndexedSeq}

object Row {
    def header(deleted: Boolean = false): Byte = {
        var header = 0x00
        val deletedFlag = if (deleted) 0 else 1
        header += deletedFlag & 0x01
        header.toByte
    }

    def isDeleted(header: Byte): Boolean = {
        (header & 0x01) == 0
    }

    def key(row: Array[Byte], schema: TableSchema): Any = {
        this.read(row, schema.keyColumn, schema)
    }

    /**
      * Parses a byte array into a row.
      * @param data binary data representing a row
      * @param schema schema of the table that owns the row
      * @return indexed sequence of the values in the row
      */
    def fromBytes(data: Array[Byte], schema: TableSchema): IndexedSeq[Any] = {
        assert(data.length == schema.rowSize, s"Byte array must have row size ${schema.rowSize} but had size ${data.length}.")
        var byteIndex = 0
        var columnIndex = 0
        val row: MIndexedSeq[Any] = MIndexedSeq.fill(schema.numColumns)(null)
        while (byteIndex < data.length) {
            val column = schema.columns(columnIndex)
            val columnBinaryData = data.slice(byteIndex, byteIndex + column.size)
            row(columnIndex) = column.fromBytes(columnBinaryData)
            columnIndex += 1
            byteIndex += column.size
        }
        row.toIndexedSeq
    }

    /**
      * Serializes a map of column names and their values into a binary row.
      * @param data map containing the column names and their values
      * @param schema schema of the table that owns the row
      * @return the byte array representing the row
      */
    def toBytes(data: List[(String, Any)], schema: TableSchema): Array[Byte] = {
        val rowData = data.toMap
        schema
            .columns
            .map { column => column.toBytes(rowData.getOrElse(column.name, null)) }
            .reduce(_ ++ _)
    }

    /**
      * Serializes the row data into a byte array.
      * @param data indexed sequence containing the row values
      * @param schema schema of the table that owns the row
      * @return the byte array representing the row
      */
    def toBytes(data: IndexedSeq[Any], schema: TableSchema): Array[Byte] = {
        schema
            .columnsWithIndex
            .map { case (column, index) => column.toBytes(data(index)) }
            .reduce(_ ++ _)
    }

    def read(row: Array[Byte], columnName: String, schema: TableSchema): Any = {
        val column = schema.columnByName(columnName)
        val offset = schema.columnOffsets(columnName)
        column.fromBytes(row.slice(offset, offset + column.size))
    }

    def write(row: Array[Byte], columnName: String, value: Any, schema: TableSchema): Unit = {
        val column = schema.columnByName(columnName)
        val offset = schema.columnOffsets(columnName)
        val binaryData = column.toBytes(value)
        System.arraycopy(binaryData, 0, row, offset, column.size)
    }

    /**
      * Applies a projection to the row and returns only the projected values. The returned values are in order of the
      * projection.
      * @param row the values of the row
      * @param projection list of columns names that should be returned
      * @param schema the schema of the table the row is part of
      * @return values of the selected columns in order of the projection
      */
    def project(row: IndexedSeq[Any], projection: IndexedSeq[String], schema: TableSchema): IndexedSeq[Any] = {
        if (projection.nonEmpty) {
            row
        } else {
            projection
                .map(schema.columnPosition)
                .map(row)
        }
    }
}
