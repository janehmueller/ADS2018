package de.hpi.ads.database.types

case class ColumnType(name: String, dataType: DataType) {
    def toBytes(data: Any): Array[Byte] = dataType.toBytes(data)

    def fromBytes(data: Array[Byte]): Any = dataType.fromBytes(data)

    def size: Int = dataType.byteSize

    def lessThan(a: Any, b: Any): Boolean = dataType.lessThan(a, b)

    def lessThanEq(a: Any, b: Any): Boolean = dataType.lessThanEq(a, b)

    def eq(a: Any, b: Any): Boolean = dataType.eq(a, b)

    def neq(a: Any, b: Any): Boolean = dataType.neq(a, b)

    def greaterThan(a: Any, b: Any): Boolean = dataType.greaterThan(a, b)

    def greaterThanEq(a: Any, b: Any): Boolean = dataType.greaterThanEq(a, b)
}

object ColumnType {
    def apply(name: String, dataType: String): ColumnType = ColumnType(name, DataType.fromString(dataType))
}
