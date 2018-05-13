package de.hpi.ads.database

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import de.hpi.ads.database.types.TableSchema

import scala.collection.mutable.{MutableList => MList}
import scala.language.dynamics

class Row(schema: TableSchema) extends Dynamic {
    val nameToIndex: Map[String, Int] = schema
        .columnNames
        .zipWithIndex
        .toMap

    val data: MList[Any] = MList.fill(schema.columns.length)(null)

    def selectDynamic(name: String): Any = this.getByName(name)

    /**
      * Returns the key of the row.
      * @return primary key of the row
      */
    def key: Any = this.data(schema.primaryKeyPosition)

    /**
      * Sets a value by its index.
      * @param index index of the value
      * @param value value that is written
      */
    def put(index: Int, value: Any): Unit = this.data(index) = value

    /**
      * Sets a value by its column name.
      * @param column name of the column
      * @param value value that is written
      */
    def putByName(column: String, value: Any): Unit = put(nameToIndex(column), value)

    /**
      * Retrieves a value by its index.
      * @param index index of the value
      * @return the value stored at the index
      */
    def get(index: Int): Any = this.data(index)

    /**
      * Retrieves a value by the column name.
      * @param column name of the column
      * @return value of the column of this row
      */
    def getByName(column: String): Any = get(nameToIndex(column))

    /**
      * Serializes the row object into a byte array.
      * @return byte array that represents the row
      */
    def toBytes: Array[Byte] = {
        val byteStream = new ByteArrayOutputStream()
        val stream = new ObjectOutputStream(byteStream)
        schema.columns.zip(this.data).foreach { case (columnType, columnData) =>
            columnType.writeBytes(columnData, stream)
        }
        stream.flush()
        val byteData = byteStream.toByteArray
        stream.close()
        byteStream.close()
        byteData
    }

    /**
      * Reads serialized values and writes them into the row data.
      * @param data serialized row that will be read
      */
    def readBytes(data: Array[Byte]): Unit = {
        val byteStream = new ByteArrayInputStream(data)
        val stream = new ObjectInputStream(byteStream)
        schema.columns.zipWithIndex.foreach { case (columnType, columnIndex) =>
            this.data(columnIndex) = columnType.readBytes(stream)
        }
        stream.close()
        byteStream.close()
    }

    /**
      * Returns this row as a list.
      */
    def toList: List[Any] = {
        this.data.toList
    }

    /**
      * Applies a projection to this row and returns only the projected values. The returned values are in order of the
      * projection.
      * @param projection list of columns names that should be returned
      * @return values of the selected columns in order of the projection
      */
    def project(projection: List[String]): List[Any] = {
        projection.map(this.getByName)
    }
}

object Row {
    def apply(schema: TableSchema): Row = new Row(schema)

    /**
      * Parses a byte array into a row.
      * @param data binary data representing a row
      * @param schema schema of the table that owns the row
      * @return the created row object
      */
    def fromBinary(data: Array[Byte], schema: TableSchema): Row = {
        val row = new Row(schema)
        row.readBytes(data)
        row
    }

    /**
      * Parses a map of column names and their values into a row.
      * @param data map containing the column names and their values
      * @param schema schema of the table that owns the row
      * @return the created row object
      */
    def fromTuples(data: List[(String, Any)], schema: TableSchema): Row = {
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
    def fromList(data: List[Any], schema: TableSchema): Row = {
        val row = new Row(schema)
        data
            .zipWithIndex
            .foreach { case (value, index) => row.put(index, value) }
        row
    }
}
