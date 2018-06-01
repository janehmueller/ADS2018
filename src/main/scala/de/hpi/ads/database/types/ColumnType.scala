package de.hpi.ads.database.types

case class ColumnType(name: String, dataType: DataType) {
    def toBytes(data: Any): Array[Byte] = dataType.toBytes(data)

    def fromBytes(data: Array[Byte]): Any = dataType.fromBytes(data)

    def size: Int = dataType.byteSize
}

object ColumnType {
    def apply(name: String, dataType: String): ColumnType = ColumnType(name, DataType.fromString(dataType))
}
