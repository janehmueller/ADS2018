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
}

object DataType {
    def fromString(dataType: String): DataType = {
        dataType.trim match {
            case "binary" => BinaryType
            case "boolean" => BooleanType
            case "date" => DateType
            case "double" => DoubleType
            case "int" => IntType
            case "string" => StringType()
            case "long" => LongType
            case unknownType => throw new IllegalArgumentException(s"Encountered unknown data type: $unknownType")
        }
    }
}
