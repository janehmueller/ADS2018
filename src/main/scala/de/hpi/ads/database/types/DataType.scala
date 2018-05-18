package de.hpi.ads.database.types

import java.io.{ObjectInputStream, ObjectOutputStream}

import de.hpi.ads.implicits._
trait DataType {
    // TODO handle null values with Options when writing and reading
    def writeBytes(data: Any, stream: ObjectOutputStream): Unit

    def readBytes(stream: ObjectInputStream): Any
}

object DataType {
    def fromString(dataType: String): DataType = {
        dataType.trim match {
            case "binary" => BinaryType
            case "boolean" => BooleanType
            case "date" => DateType
            case "double" => DoubleType
            case "int" => IntType
            case "string" => StringType
            case "long" => LongType
            case r"""list<([a-z]+)${secondType}>""" => ListType(this.fromString(secondType))
            case r"""map<([a-z]+)${keyType} *, *([a-z<>, ]+)${valueType}>""" =>
                MapType(this.fromString(keyType), this.fromString(valueType))
            case unknownType => throw new IllegalArgumentException(s"Encountered unknown data type: $unknownType")
        }
    }
}
