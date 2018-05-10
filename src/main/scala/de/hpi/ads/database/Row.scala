package de.hpi.ads.database

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import de.hpi.ads.database.types.TableSchema

import scala.collection.mutable.{Map => MMap}

class Row(schema: TableSchema) {
    // TODO serialization that enables deserialization without knowledge of index
    val nameToIndex: Map[String, Int] = schema.columns
        .map(attribute => (attribute, schema.columnPosition(attribute)))
        .toMap

    var data: MMap[Int, String] = MMap.empty

    /**
      * Returns the key of the row.
      * @return primary key of the row
      */
    def key: Any = data(schema.primaryKeyPosition)

    /**
      * Sets a value by its index.
      * @param index index of the value
      * @param value value that is written
      */
    def put(index: Int, value: String): Unit = data(index) = value

    /**
      * Sets a value by its column name.
      * @param column name of the column
      * @param value value that is written
      */
    def putByName(column: String, value: String): Unit = put(nameToIndex(column), value)

    /**
      * Retrieves a value by its index.
      * @param index index of the value
      * @return the value stored at the index
      */
    def get(index: Int): String = data(index)

    /**
      * Retrieves a value by the column name.
      * @param column name of the column
      * @return value of the column of this row
      */
    def getByName(column: String): String = get(nameToIndex(column))

    /**
      * Serializes the row object into a byte array.
      * @return byte array that represents the row
      */
    def toBytes: Array[Byte] = {
        val byteStream = new ByteArrayOutputStream()
        val stream = new ObjectOutputStream(byteStream)
        (0 until schema.numValues).foreach { index =>
            val stringAsBytes = data.getOrElse(index, "").getBytes(Row.stringEncoding)
            stream.writeInt(stringAsBytes.length)
            stream.write(stringAsBytes)
        }
        stream.flush()
        val byteData = byteStream.toByteArray
        stream.close()
        byteStream.close()
        byteData
    }

    /**
      * Returns this row as a list.
      */
    def toList: List[Any] = {
        data
            .toList
            .sortBy { case (index, value) => index }
            .map { case (index, value) => value }
    }
}

object Row {
    val stringEncoding = "UTF-8"

    def apply(schema: TableSchema): Row = new Row(schema)

    /**
      * Parses a byte array into a row.
      * @param data binary data representing a row
      * @param schema schema of the table that owns the row
      * @return the created row object
      */
    def fromBinary(data: Array[Byte], schema: TableSchema): Row = {
        // TODO this seems extremely slow
        val row = new Row(schema)
        val byteStream = new ByteArrayInputStream(data)
        val stream = new ObjectInputStream(byteStream)
        (0 until schema.numValues).foreach { index =>
            val length = stream.readInt()
            val stringAsBytes = new Array[Byte](length)
            stream.readFully(stringAsBytes)
            row.data(index) = new String(stringAsBytes, stringEncoding)
        }
        stream.close()
        byteStream.close()
        row
    }

    /**
      * Parses a map of column names and their values into a row.
      * @param data map containing the column names and their values
      * @param schema schema of the table that owns the row
      * @return the created row object
      */
    def fromMap(data: Map[String, String], schema: TableSchema): Row = {
        val row = new Row(schema)
        data.foreach((row.putByName _).tupled)
        row
    }

    /**
      * Parses a list of values into a row.
      * @param data list containing the values
      * @param schema schema of the table that owns the row
      * @return the created row object
      */
    def fromList(data: List[String], schema: TableSchema): Row = {
        val row = new Row(schema)
        data
            .zipWithIndex
            .foreach { case (value, index) => row.put(index, value) }
        row
    }
}
