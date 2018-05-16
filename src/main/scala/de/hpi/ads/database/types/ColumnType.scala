package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

case class ColumnType(name: String, dataType: DataType) {
    def writeBytes(data: Any, stream: ObjectOutputStream): Unit = dataType.writeBytes(data, stream)

    def readBytes(stream: ObjectInputStream): Any = dataType.readBytes(stream)
}

object ColumnType {
    def apply(name: String, dataType: String): ColumnType = ColumnType(name, DataType.fromString(dataType))
}
