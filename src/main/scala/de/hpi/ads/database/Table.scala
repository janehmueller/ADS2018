package de.hpi.ads.database

import java.io.RandomAccessFile

import de.hpi.ads.database.types.TableSchema

import scala.collection.immutable.NumericRange
import scala.collection.mutable.{Map => MMap, Set => MSet}

class Table(fileName: String, schemaString: String) {
    /**
      * The file to which the table data is saved.
      */
    var tableFile: RandomAccessFile = new RandomAccessFile(fileName, "rw")

    val schema = new TableSchema(schemaString)

    /**
      * Stores offset and length for row keys.
      * TODO: serialize
      */
    val keyPositions: MMap[Any, (Long, Int)] = MMap.empty

    // TODO: more data types than string

    /**
      * Contains ranges of unused memory in the table file.
      * F: I don't think we ever want this.
      */
    var freeMemory: MSet[NumericRange[Long]] = MSet.empty

    /**
      * Reads the row for a given key from the table file.
      * @param key the key of the row
      * @return the row if its found or None if the row key is not present
      */
    def select(key: Any): Option[Row] = {
        // if the key is not in the index it does not exist
        keyPositions
            .get(key)
            .map { case (offset, length) =>
                val rowData = readRow(offset, length)
                Row.fromBinary(rowData, schema)
            }
    }

    /**
      * Reads a row as a byte array from the table file.
      * @param offset byte offset in the file
      * @param length number of bytes to read
      * @return the read byte array representing the row
      */
    def readRow(offset: Long, length: Int): Array[Byte] = {
        val byteBuffer = new Array[Byte](length)
        tableFile.seek(offset)
        tableFile.readFully(byteBuffer)
        byteBuffer
    }

    /**
      * Inserts a new entry into the table. The passed key should not already exist in the table.
      * @param data map of attribute name and attribute value that must contain at least the key
      */
    def insert(data: Map[String, String]): Unit = {
        assert(data.contains(schema.key), "A new entry must contain at least the primary key.")
        val row = Row.fromMap(data, schema)
        insertRow(row)
    }

    /**
      * Inserts a new entry into the table. Assumes the passed data is in order of the attributes. Can miss trailing
      * columns.
      * @param data list of column values of the new entry
      */
    def insertList(data: List[String]): Unit = {
        val dataWithColumns = schema.columns.zip(data).toMap
        insert(dataWithColumns)
    }

    /**
      * Inserts a new entry into the table. The passed key should not already exist in the table.
      * @param row row object of the inserted data
      */
    def insertRow(row: Row): Unit = {
        assert(!keyPositions.contains(row.key), "A new entry must contain primary key that does not already exist.")
        val byteData = row.toBytes
        val length = byteData.length
        val offset = appendRow(byteData)
        keyPositions(row.key) = (offset, length)
    }

    /**
      * Appends a row in the binary format to the table.
      * @param row row as a byte array
      * @return the memory offset of the appended row in the table
      */
    def appendRow(row: Array[Byte]): Long = {
        val memoryPosition = tableFile.length()
        tableFile.seek(memoryPosition)
        tableFile.write(row)
        memoryPosition
    }

    /**
      * Updates a row by overwriting it with a new row object.
      * @param row new row that overwrites an already existing row
      */
    def update(row: Row): Unit = {
        //TODO this is not the expected behavior of an "update"
        delete(row.key)
        insertRow(row)
    }

    /**
      * Deletes entry of the primary key.
      * @param key primary key of the deleted row
      */
    def delete(key: Any): Unit = {
        // TODO: handle free memory in file
        keyPositions
            .remove(key)
            .map { case (offset, length) =>
                freeMemory += NumericRange(offset, offset + length, 1L)
            }
    }
}

object Table {
    def apply(fileName: String, schema: String): Table = new Table(fileName, schema)
}
