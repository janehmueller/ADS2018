package de.hpi.ads.database.types

import de.hpi.ads.implicits._

trait DataType {
    /**
      * Byte conversions from
      * https://www.daniweb.com/programming/software-development/code/216874/primitive-types-as-byte-arrays
      */
    def toBytes(data: Any): Array[Byte]

    def fromBytes(data: Array[Byte]): Any

    def testInputLength(data: Array[Byte]): Unit = {
        assert(data.length == this.byteSize, s"Binary data must have size ${this.byteSize} but had size ${data.length}.")
    }

    def byteSize: Int

    def lessThan(a: Any, b: Any): Boolean

    def lessThanEq(a: Any, b: Any): Boolean = lessThan(a, b) || eq(a, b)

    def eq(a: Any, b: Any): Boolean = a.getClass == b.getClass && a == b

    def neq(a: Any, b: Any): Boolean = !eq(a, b)

    def greaterThan(a: Any, b: Any): Boolean = !lessThanEq(a, b)

    def greaterThanEq(a: Any, b: Any): Boolean = !lessThan(a, b)

    def max(values: Any*): Any

    def min(values: Any*): Any

    def avg(value1: Any, value2: Any): Any
}

@SerialVersionUID(100L)
object DataType extends Serializable {
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
