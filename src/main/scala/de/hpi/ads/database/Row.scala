package de.hpi.ads.database

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import de.hpi.ads.database.types.Schema

import scala.collection.mutable.{Map => MMap}

class Row(schema: Schema) {

    val nameToIndex: Map[String, Int] = schema.attributes
        .map(attribute => (attribute, schema.indexOfAttribute(attribute)))
        .toMap

    var data: MMap[Int, String] = MMap.empty

    /**
      * Returns the key of the row.
      * @return primary key of the row
      */
    def key: Any = data(schema.keyIndex)

    /**
      * Retrieves a value by its index.
      * @param index index of the value
      * @return the value stored at the index
      */
    def get(index: Int): String = data(index)

    /**
      * Retrieves a value by the attribute name.
      * @param attribute name of the attribute
      * @return value of the attribute
      */
    def getByName(attribute: String): String = get(nameToIndex(attribute))

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
}

object Row {
    val stringEncoding = "UTF-8"

    def apply(schema: Schema): Row = new Row(schema)

    /**
      * Parses a byte array into a row.
      * @param data binary data representing a row
      * @param schema schema of the table that owns the row
      * @return the created row object
      */
    def fromBinary(data: Array[Byte], schema: Schema): Row = {
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
      * Parses a map of attributes and their values into a row.
      * @param data map containing the attributes and their values
      * @param schema schema of the table that owns the row
      * @return the created row object
      */
    def fromMap(data: Map[String, String], schema: Schema): Row = {
        val row = new Row(schema)
        data.foreach { case (attribute, value) =>
            row.data(row.nameToIndex(attribute)) = value
        }
        row
    }
}
