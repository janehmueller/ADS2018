package de.hpi.ads.database

import java.io.RandomAccessFile
import java.nio.file.{Files, Paths}

import de.hpi.ads.database.types.TableSchema

import scala.collection.mutable.{Map => MMap}


class Table(fileName: String, schema: TableSchema) {
    /**
      * The file to which the table data is saved.
      */
    var tableFile: RandomAccessFile = _

    /**
      * Stores offset for row keys.
      * TODO: serialize
      */
    val keyPositions: MMap[Any, Long] = MMap.empty

//    /**
//      * Contains ranges of unused memory in the table file.
//      * F: I don't think we ever want this.
//      * TODO: compaction
//      */
//    var freeMemory: MSet[NumericRange[Long]] = MSet.empty

    /** Initialize object */
    this.openTableFile()

    def openTableFile(): Unit = {
        // TODO: maybe serialize schema and read it again
        val fileExists = Files.exists(Paths.get(fileName))
        this.tableFile = new RandomAccessFile(fileName, "rw")
        if (fileExists) {
            this.rebuildIndex()
        }
    }

    def rebuildIndex(): Unit = {
        this.keyPositions.empty
        this.tableFile.seek(0)
        while (this.tableFile.getFilePointer < this.tableFile.length()) {
            val offset = this.tableFile.getFilePointer
            val row = this.readNextRow
            this.keyPositions(row.key) = offset
        }
    }

    def cleanUp(): Unit = {
        tableFile.close()
        Files.deleteIfExists(Paths.get(fileName))
    }

    def readFile(): Array[Byte] = {
        assert(tableFile.length() < 2147483647)
        val data: Array[Byte] = Array.ofDim[Byte](tableFile.length().toInt)
        tableFile.seek(0)
        tableFile.read(data)
        data
    }

    /**
      * Returns two roughly equally sized Bytearrays that are valid files for new tables and splitting point (part of second partition)
     */
    def readFileHalves(): (Array[Byte], Array[Byte], Any) = {
        val primaryKeyMedian = getPrimaryKeyMedian()
        //TODO when fixed row size is implemented
        (Array[Byte](), Array[Byte](), primaryKeyMedian)
    }

    /**
      * Reads the row for a given key from the table file.
      * @param key the key of the row
      * @return the row if its found or None if the row key is not present
      */
    def select(key: Any): Option[Row] = {
        // if the key is not in keyPositions it does not exist
        this.keyPositions
            .get(key)
            .map(readRow)
    }

    /**
      * Reads the rows for which the given query evaluates as true.
      * @param query the query that decides which rows are returned
      * @return the rows for which the query evaluates as true
      */
    def selectWhere(query: Row => Boolean): List[Row] = {
        this.readRows(query)
    }

    /**
      * Reads a row from the table file.
      * @param offset byte offset in the file
      * @return the read row
      */
    def readRow(offset: Long): Row = {
        tableFile.seek(offset)
        val numBytes = tableFile.readInt()
        val binaryRow = new Array[Byte](numBytes)
        tableFile.readFully(binaryRow)
        Row.fromBinary(binaryRow, schema)
    }

    /**
      * Reads a row starting at the current file pointer position.
      * @return the read row
      */
    def readNextRow: Row = {
        val numBytes = tableFile.readInt()
        val binaryRow = new Array[Byte](numBytes)
        tableFile.readFully(binaryRow)
        Row.fromBinary(binaryRow, schema)
    }

    //TODO this is very inefficient, there should be only one read that reads everything
    def readRows(query: Row => Boolean = _ => true): List[Row] = {
        tableFile.seek(0)
        // Use a map to skip rows that were updated.
        val readRows = MMap.empty[Any, Row]
        while (tableFile.getFilePointer < tableFile.length()) {
            val row = this.readNextRow
            if (query(row)) {
                readRows(row.key) = row
            }
        }
        readRows.values.toList
    }

    /**
      * Inserts a new entry into the table. The passed values must contain a primary key that does not already exist.
      * @param data list of attribute names and and their values
      */
    def insert(data: List[(String, Any)]): Unit = {
        assert(data.exists(_._1 == schema.keyColumn), "A new entry must contain the primary key.")
        val row = Row.fromTuples(data, schema)
        insertRow(row)
    }

    /**
      * Inserts a new entry into the table. Assumes the passed data is in order of the attributes. Can miss trailing
      * columns.
      * @param data list of column values of the new entry
      */
    def insertList(data: List[Any]): Unit = {
        assert(data.length > schema.primaryKeyPosition, "A new entry must contain the primary key.")
        val row = Row.fromList(data, schema)
        insertRow(row)
    }

    /**
      * Inserts a new entry into the table. The passed key should not already exist in the table.
      * @param row row object of the inserted data
      */
    def insertRow(row: Row): Unit = {
        assert(!keyPositions.contains(row.key), "A new entry must contain primary key that does not already exist.")
        val byteData = row.toBytes
        val offset = appendBinaryRow(byteData)
        keyPositions(row.key) = offset
    }

    /**
      * Appends a row in the binary format to the table. The row binary data is prepended by its length as a 32 bit int
      * @param row row as a byte array
      * @return the memory offset of the appended row in the table
      */
    def appendBinaryRow(row: Array[Byte]): Long = {
        val memoryPosition = tableFile.length()
        tableFile.seek(memoryPosition)
        tableFile.writeInt(row.length)
        tableFile.write(row)
        memoryPosition
    }

    def rebuildTableFromData(data: Array[Byte]): Unit = {
        assert(tableFile.length() == 0)
        tableFile.write(data)
        rebuildIndex()
    }

    /**
      * Updates a row by overwriting it with the passed data.
      * @param key the key of the row
      * @param data list of attribute names and and their values that will be updated
      */
    def update(key: Any, data: List[(String, Any)]): Unit = {
        val updatedRow = this
            .select(key)
            .map { row =>
                data.foreach((row.putByName _).tupled)
                row
            }
        // TODO: write row into existing space if it does not get larger
        delete(key)
        updatedRow.foreach(insertRow)
    }

    /**
      * Update a rows for which the query evaluates as true with the passed data.
      * @param data list of attribute names and and their values that will be updated
      * @param query the query deciding which rows will be updated
      */
    def updateWhere(data: List[(String, Any)], query: Row => Boolean): Unit = {
        val updatedRows = this
            .selectWhere(query)
            .map { row =>
                data.foreach((row.putByName _).tupled)
                row
            }
        // TODO: write row into existing space if it does not get larger
        updatedRows.foreach(row => delete(row.key))
        updatedRows.foreach(insertRow)
    }

    /**
      * Deletes entry of the primary key.
      * @param key primary key of the deleted row
      */
    def delete(key: Any): Unit = {
        // TODO: handle free memory in file
        keyPositions.remove(key)
    }

    /**
      * Deletes entry of the primary key.
      * @param query the query deciding which rows will be deleted
      */
    def deleteWhere(query: Row => Boolean): Unit = {
        // TODO: handle free memory in file
        // TODO: find a better way
        this.selectWhere(query).foreach(row => this.keyPositions.remove(row.key))
    }



    def getPrimaryKeyMedian(): Any = {
        def medianUpTo5(five: Array[Any], lt: (Any, Any) => Boolean): Any = {
            def order2(a: Array[Any], i: Int, j: Int, lt: (Any, Any) => Boolean) = {
                if (lt(a(j),a(i))) { val t = a(i); a(i) = a(j); a(j) = t }
            }

            def pairs(a: Array[Any], i: Int, j: Int, k: Int, l: Int, lt: (Any, Any) => Boolean) = {
                if (lt(a(i),a(k))) { order2(a,j,k,lt); a(j) }
                else { order2(a,i,l,lt); a(i) }
            }

            if (five.length < 2) return five(0)
            order2(five,0,1,lt)
            if (five.length < 4) return (
                if (five.length==2 || lt(five(2) , five(0))) five(0)
                else if (lt(five(1) , five(2))) five(1)
                else five(2)
                )
            order2(five,2,3,lt)
            if (five.length < 5) pairs(five,0,1,2,3,lt)
            else if (lt(five(0) , five(2))) { order2(five,1,4,lt); pairs(five,1,4,2,3,lt) }
            else { order2(five,3,4,lt); pairs(five,0,1,3,4,lt) }
        }

        def medianOfMedians(arr: Array[Any], lt: (Any, Any) => Boolean): Any = {
            val medians = (arr grouped 5).map(x => medianUpTo5(x, lt)).toArray
            if (medians.length <= 5) medianUpTo5 (medians, lt)
            else medianOfMedians(medians, lt)
        }

        val primaryKeyValues = keyPositions.keys.toArray
        medianOfMedians(primaryKeyValues, schema.primaryKeyColumn.dataType.lessThan)
    }

}

object Table {
    def apply(fileName: String, schema: TableSchema): Table = new Table(fileName, schema)

    def fromSchemaString(fileName: String, schema: String): Table = new Table(fileName, TableSchema(schema))
}
