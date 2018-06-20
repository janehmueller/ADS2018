package de.hpi.ads.database

import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths}

import de.hpi.ads.database.operators.Operator
import de.hpi.ads.database.types.TableSchema
import de.hpi.ads.utils.medianOfMedians

import scala.collection.mutable.{Map => MMap, Set => MSet}
import util.control.Breaks._

class Table(fileName: String, schema: TableSchema) {
    /**
      * The file to which the table data is saved.
      */
    var tableFile: RandomAccessFile = _

//    var memoryMappedTable: MappedByteBuffer = _
//
//    var length: Int = 0

    /**
      * Stores offset for row keys.
      */
    val keyPositions: MMap[Any, Long] = MMap.empty

    var nonKeyIndices: MMap[String, MMap[Any, List[Long]]] = MMap.empty

    def hasIndex(column: String): Boolean = {
        nonKeyIndices.keySet(column) || this.schema.keyColumn == column
    }

    /**
      * Contains offsets of unused memory in the table file.
      */
    var freeMemory: MSet[Long] = MSet.empty

    /** Initialize object */
    this.openTableFile()

    def openTableFile(): Unit = {
        val fileExists = Files.exists(Paths.get(fileName))
        this.tableFile = new RandomAccessFile(fileName, "rw")
//        this.memoryMappedTable = this.tableFile.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024 * 1024)
        if (fileExists) {
            this.rebuildIndex()
        }
    }

    def rebuildIndex(): Unit = {
        this.keyPositions.empty
        val binaryData = this.readFile
        val rowSize = this.schema.rowSizeWithHeader
        assert(binaryData.length % rowSize == 0, s"Binary data can not be evenly split in $rowSize sized chunks.")
        binaryData
            .grouped(rowSize)
            .zipWithIndex
            .flatMap { case (binaryRow, index) =>
                val header = binaryRow(0)
                val key = Row.read(binaryRow.slice(1, rowSize), schema.keyColumn, this.schema)
                if (Row.isDeleted(header)) None else Option((key, index))
            }.foreach { case (key, index) =>
                val offset = rowSize * index
                this.keyPositions(key) = offset
            }
    }

    def cleanUp(): Unit = {
        releaseFile()
        Files.deleteIfExists(Paths.get(fileName))
    }

    def releaseFile(): Unit = {
        tableFile.close()
    }

    def readFile: Array[Byte] = {
        assert(tableFile.length <= Integer.MAX_VALUE)
        val data = new Array[Byte](tableFile.length.toInt)
        tableFile.seek(0)
        tableFile.readFully(data)
        data
//        this.memoryMappedTable.position(0)
//        val data = new Array[Byte](this.length)
//        this.memoryMappedTable.get(data)
//        data
    }

    /**
      * Splits the table file in roughly equally sized halves.
      * @return both table halves as byte-arrays and the key of the first row of the second part
      */
    def readFileHalves: (Array[Byte], Array[Byte], Any) = {
        val primaryKeyMedian = this.getPrimaryKeyMedian
        val fileHalves = readFileHalves(primaryKeyMedian)
        (fileHalves._1, fileHalves._2, primaryKeyMedian)
    }

    def readFileHalves(primaryKeyMedian: Any): (Array[Byte], Array[Byte]) = {
        val binaryData = this.readFile
        val rowSize = this.schema.rowSizeWithHeader
        var leftHalf = Array[Byte]()
        var rightHalf = Array[Byte]()
        var i = 0
        while (i < binaryData.length) {
            breakable {
                val rowWithHeader = binaryData.slice(i, i + rowSize)
                val header = rowWithHeader(0)
                if (Row.isDeleted(header)) {
                    break
                }
                val binaryRow = rowWithHeader.slice(1, rowWithHeader.length)
                val rowKey = Row.read(binaryRow, this.schema.keyColumn, this.schema)
                if (this.schema.primaryKeyColumn.lessThan(rowKey, primaryKeyMedian)) {
                    leftHalf ++= rowWithHeader
                } else {
                    rightHalf ++= rowWithHeader
                }
            }
            i += rowSize
        }
        (leftHalf, rightHalf)
    }

    /**
      * Appends a row in the binary format to the table. The row binary data is prepended by its length as a 32 bit int
      * @param row row as a byte array
      * @return the memory offset of the appended row in the table
      */
    def insertBinaryRow(row: Array[Byte]): Long = {
        val memoryPosition = this.freeMemory.headOption.getOrElse(tableFile.length)
//        val memoryPosition = this.freeMemory.headOption.getOrElse(this.length.toLong)
        this.overwriteBinaryRow(row, memoryPosition)
        memoryPosition
    }

    def overwriteBinaryRow(row: Array[Byte], offset: Long): Unit = {
//        this.memoryMappedTable.position(offset.toInt)
//        this.memoryMappedTable.put(Row.header())
//        this.memoryMappedTable.put(row)
//        if (offset.toInt == this.length) {
//            this.length += row.length + 1
//        }
//        this.memoryMappedTable.force()
        tableFile.seek(offset)
        tableFile.writeByte(Row.header())
        tableFile.write(row)
    }

    def deleteBinaryRow(offset: Long): Unit = {
//        this.memoryMappedTable.position(offset.toInt)
//        this.memoryMappedTable.put(Row.header(deleted = true))
//        this.memoryMappedTable.force()
        tableFile.seek(offset)
        tableFile.writeByte(Row.header(deleted = true))
    }

    def rebuildTableFromData(data: Array[Byte]): Unit = {
//        this.memoryMappedTable.position(0)
//        this.memoryMappedTable.put(data)
//        this.length = data.length
//        this.memoryMappedTable.force()
        assert(tableFile.length() == 0)
        tableFile.write(data)
        rebuildIndex()
    }

    /**
      * Reads the row for a given key from the table file.
      * @param key the key of the row
      * @return the row if its found or None if the row key is not present
      */
    def select(key: Any): Option[Array[Byte]] = {
        // if the key is not in keyPositions it does not exist
        this.keyPositions
            .get(key)
            .map(readRow)
    }

    /**
      * Reads the rows for which the given query evaluates as true.
      * @param operator the operator that decides which rows will be returned
      * @return the rows for which the query evaluates as true
      */
    def selectWhere(operator: Operator): List[Array[Byte]] = {
        var result: List[Array[Byte]] = Nil
        if (this.hasIndex(operator.column)) {
            var memoryLocations: List[Long] = Nil
            // Use either key index or non-key index
            if (operator.column == this.schema.keyColumn) {
                memoryLocations ++= operator.useKeyIndex(this.keyPositions, schema)
            } else {
                memoryLocations ++= operator(this.nonKeyIndices(operator.column), schema)
            }

            // Only use index with random I/O when we read less than half of this partitions entries
            if (memoryLocations.length < this.keyPositions.size / 2) {
                result = memoryLocations.map(this.readRow)
            } else {
                result = this.readRows(row => operator(row, this.schema))
            }
        } else {
            result = this.readRows(row => operator(row, this.schema))
        }
        result
    }

    /**
      * Reads a row from the table file.
      * @param offset byte offset in the file
      * @return the read row
      */
    def readRow(offset: Long): Array[Byte] = {
//        this.memoryMappedTable.position(offset.toInt)
//        val header = this.memoryMappedTable.get()
//        val row = new Array[Byte](this.schema.rowSize)
//        this.memoryMappedTable.get(row)
//        row
        tableFile.seek(offset)
        val header = tableFile.readByte()
        val row = new Array[Byte](this.schema.rowSize)
        tableFile.readFully(row)
        row
    }

    /**
      * Reads a row starting at the current file pointer position.
      * @return the read row
      */
    def readNextRow: Array[Byte] = {
//        val header = this.memoryMappedTable.get()
//        val row = new Array[Byte](this.schema.rowSize)
//        this.memoryMappedTable.get(row)
//        row
        val header = tableFile.readByte()
        val row = new Array[Byte](this.schema.rowSize)
        tableFile.readFully(row)
        row
    }

    def readRows(query: Array[Byte] => Boolean = _ => true): List[Array[Byte]] = {
        val binaryData = this.readFile
        val rowSize = this.schema.rowSizeWithHeader
        assert(binaryData.length % rowSize == 0)
        binaryData
            .grouped(rowSize)
            .flatMap { binaryRow =>
                val header = binaryRow(0)
                val row = binaryRow.slice(1, rowSize)
                if (Row.isDeleted(header)) None else Option(row)
            }.filter(query)
            .toList
    }

    /**
      * Inserts a new entry into the table. The passed values must contain a primary key that does not already exist.
      * @param data list of attribute names and and their values
      */
    def insert(data: List[(String, Any)]): Unit = {
        assert(data.exists(_._1 == schema.keyColumn), "A new entry must contain the primary key.")
        val row = Row.toBytes(data, schema)
        insertRow(row)
    }

    /**
      * Inserts a new entry into the table. Assumes the passed data is in order of the attributes. Can miss trailing
      * columns.
      * @param data list of column values of the new entry
      */
    def insertList(data: List[Any]): Unit = {
        assert(data.length > schema.primaryKeyPosition, "A new entry must contain the primary key.")
        val row = Row.toBytes(data.toIndexedSeq, schema)
        insertRow(row)
    }

    /**
      * Inserts a new entry into the table. The passed key should not already exist in the table.
      * @param row binary row of the inserted data
      */
    def insertRow(row: Array[Byte]): Unit = {
        val key = Row.key(row, this.schema)
        assert(!keyPositions.contains(key), "A new entry must contain primary key that does not already exist.")
        val offset = insertBinaryRow(row)
        keyPositions(key) = offset
    }

    /**
      * Updates a row by overwriting it with the passed data.
      * @param key the key of the row
      * @param data list of attribute names and and their values that will be updated
      */
    def update(key: Any, data: List[(String, Any)]): Unit = {
        val row = this.select(key)
        if(row.isEmpty) {
            return
        }
        val updatedRow = row.get
        data.foreach { case (column, value) => Row.write(updatedRow, column, value, this.schema) }
        this.updateRow(updatedRow)
    }

    /**
      * Update a rows for which the query evaluates as true with the passed data.
      * @param data list of attribute names and and their values that will be updated
      * @param operator the operator deciding which rows will be updated
      */
    def updateWhere(data: List[(String, Any)], operator: Operator): Unit = {
        val updatedRows = this
            .selectWhere(operator)
            .map { row =>
                data.foreach { case (column, value) => Row.write(row, column, value, this.schema) }
                row
            }
        updatedRows.foreach(this.updateRow)
    }

    def updateRow(row: Array[Byte]): Unit = {
        val key = Row.key(row, this.schema)
        assert(keyPositions.contains(key), "An updated entry must contain an existing primary key.")
        val offset = this.keyPositions(key)
        this.overwriteBinaryRow(row, offset)
    }

    /**
      * Deletes entry of the primary key.
      * @param key primary key of the deleted row
      */
    def delete(key: Any): Unit = {
        val offset = keyPositions.remove(key)
        offset.foreach(this.deleteBinaryRow)
        offset.foreach(this.freeMemory += _)
    }

    /**
      * Deletes entry of the primary key.
      * @param operator the query operator which rows will be deleted
      */
    def deleteWhere(operator: Operator): Unit = {
        this.selectWhere(operator).foreach(row => this.delete(Row.key(row, this.schema)))
    }

    def getPrimaryKeyMedian: Any = {
        val primaryKeyValues = keyPositions.keys.toArray
        medianOfMedians(primaryKeyValues, schema.primaryKeyColumn.dataType.lessThan)
    }
}

object Table {
    def apply(fileName: String, schema: TableSchema): Table = new Table(fileName, schema)

    def fromSchemaString(fileName: String, schema: String): Table = new Table(fileName, TableSchema(schema))
}
