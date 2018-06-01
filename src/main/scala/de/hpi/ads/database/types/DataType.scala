package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

import de.hpi.ads.implicits._

trait DataType {
    // TODO handle null values with Options when writing and reading
    def writeBytes(data: Any, stream: ObjectOutputStream): Unit

    def readBytes(stream: ObjectInputStream): Any

    /**
      * Byte conversions from
      * https://www.daniweb.com/programming/software-development/code/216874/primitive-types-as-byte-arrays
      */
    def toBytes(data: Any): Array[Byte]

    def fromBytes(data: Array[Byte]): Any

    def byteSize: Int

    def lessThan(a: Any, b: Any): Boolean

    def lessThanEq(a: Any, b: Any): Boolean = lessThan(a, b) || eq(a, b)

    def eq(a: Any, b: Any): Boolean = a.getClass == b.getClass && a == b

    def greaterThan(a: Any, b: Any): Boolean = !lessThanEq(a, b)

    def greaterThanEq(a: Any, b: Any): Boolean = !lessThan(a, b)

    def max(values: Any*): Any

    def min(values: Any*): Any
}

object DataType {
    def fromString(dataType: String): DataType = {
        dataType.trim match {
            case r"""binary\(([0-9]+)${length}\)""" => BinaryType(length.toInt)
            case "boolean" => BooleanType
            case "date" => DateType
            case "double" => DoubleType
            case "int" => IntType
            case "long" => LongType
            case r"""string\(([0-9]+)${length}\)""" => StringType(length.toInt)
            case unknownType => throw new IllegalArgumentException(s"Encountered unknown data type: $unknownType")
        }
    }
}
