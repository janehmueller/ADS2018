package de.hpi.ads.database

import java.io.{File, RandomAccessFile}


import de.hpi.ads.database.types.TableSchema


import scala.collection.mutable.{ListBuffer}


class Table(fileName: String, schema: TableSchema) {
    /**
      * The file to which the table data is saved.
      */
    var tableFile: RandomAccessFile = new RandomAccessFile(fileName, "rw")


    /**
      * Inserts a new entry into the table. The passed key should not already exist in the table.
      * @param data map of attribute name and attribute value that must contain at least the key
      */
    def insert(data: Map[String, String]): Unit = {
        assert(false)
        //assert(data.contains(schema.key), "A new entry must contain at least the primary key.")
        //val row = Row.fromMap(data, schema)
        //insertRow(row)
    }

    /**
      * Inserts a new entry into the table. Assumes the passed data is in order of the attributes.
      * @param data list of column values of the new entry
      */
    def insertList(data: List[Any]): Unit = {
        assert(data.length == schema.numberOfColumns, "Data length does not equal the expected number of columns")
        val memoryPosition = tableFile.length()
        tableFile.seek(memoryPosition)
        for (i <- data.indices) {
            schema.columnDataTypes(i) match {
                case Boolean => tableFile.writeBoolean(data(i).asInstanceOf[Boolean])
                case Int => tableFile.writeInt(data(i).asInstanceOf[Int])
                case Long => tableFile.writeLong(data(i).asInstanceOf[Long])
                case Double => tableFile.writeDouble(data(i).asInstanceOf[Double])
                case "S" => tableFile.writeBytes(String.format("%1$"+schema.columnSizes(i)+"s", data(i).asInstanceOf[String]+'\n'))
                case _ => assert(false, "Data type not supported")
            }
        }
    }

    //reads one row from current file pointer position
    def readRow(): List[Any] = {
        val result: ListBuffer[Any] = new ListBuffer[Any]
        for (i <- schema.columnDataTypes.indices) {
            schema.columnDataTypes(i) match {
                case Boolean => result += tableFile.readBoolean()
                case Int => result += tableFile.readInt()
                case Long => result += tableFile.readLong()
                case Double => result += tableFile.readDouble()
                case "S" => result += tableFile.readLine().trim
                case _ => assert(false, "Data type not supported")
            }
        }
        result.toList
    }

    //returns all rows in the table
    def readAll(): List[List[Any]] = {
        val result: ListBuffer[List[Any]]  = new ListBuffer[List[Any]]
        tableFile.seek(0)
        while (tableFile.getFilePointer != tableFile.length()) {
            result += this.readRow
        }
        result.toList
    }

    //takes rows that conform to the table schema and shortens them
    def projectRows(projection: List[String], rows: List[List[Any]]): List[List[Any]] = {
        val result: ListBuffer[List[Any]]  = new ListBuffer[List[Any]]
        val resultRows: Array[Boolean] = Array.fill[Boolean](schema.columnNames.size)(false)
        //TODO unless the compiler can optimize this, this is slow
        var j: Int = 0
        for (i <- schema.columnNames.indices) {
            if (j < projection.size && projection(j) == schema.columnNames(i)) {
                j += 1
                resultRows(i) = true
            }
        }

        for (i <- rows.indices) {
            val r = new ListBuffer[Any]
            rows(i).zipWithIndex.foreach { case (rowItem, j) =>
                if(resultRows(j)) {
                    r += rowItem
                }
            }
            result += r.toList
        }
        result.toList
    }

    //primitive filtering without index
    def selectWhere(conditions: List[Any] => Boolean): List[List[Any]] = {
        readAll().filter(conditions)
    }

    def cleanUp() = {
        tableFile.close()
        val fileTemp = new File(fileName)
        if (fileTemp.exists) {
            fileTemp.delete()
        }
    }
}

object Table {
    def apply(fileName: String, schema: TableSchema): Table = new Table(fileName, schema)
}
