package de.hpi.ads.database.types

import java.util.Arrays

@SerialVersionUID(106L)
case class BinaryType(length: Int = 255) extends DataType {
    override def byteSize: Int = length

    // TODO prepend length
    override def toBytes(data: Any): Array[Byte] = {
        val binaryData = data.asInstanceOf[Array[Byte]]
        Arrays.copyOf(binaryData, this.length)
    }

    override def fromBytes(data: Array[Byte]): Array[Byte] = {
        Arrays.copyOf(data, this.length)
    }

    override def lessThan(a: Any, b: Any): Boolean = {
        throw new UnsupportedOperationException("Binary data type does not support comparisons.")
    }

    override def max(values: Any*): Array[Byte] = {
        throw new UnsupportedOperationException("Binary data type does not support comparisons.")
    }

    override def min(values: Any*): Array[Byte] = {
        throw new UnsupportedOperationException("Binary data type does not support comparisons.")
    }

    override def avg(value1: Any, value2: Any): Any = {
        throw new UnsupportedOperationException("This data type does not support averages.")
    }
}
