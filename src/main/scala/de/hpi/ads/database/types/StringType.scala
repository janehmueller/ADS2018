package de.hpi.ads.database.types

import java.util.Arrays

@SerialVersionUID(104L)
case class StringType(length: Int = 255) extends DataType with Serializable {
    override def byteSize: Int = length * 4

    // TODO prepend length
    override def toBytes(data: Any): Array[Byte] = {
        val binaryData = data.asInstanceOf[String].getBytes(StringType.encoding)
        Arrays.copyOf(binaryData, this.byteSize)
    }

    override def fromBytes(data: Array[Byte]): String = {
        // remove null byte padding
        var i = data.length - 1
        while (i >= 0 && data(i) == 0) {
            i -= 1
        }
        new String(data.slice(0, i + 1), StringType.encoding)
    }

    override def lessThan(a: Any, b: Any): Boolean = a.asInstanceOf[String] < b.asInstanceOf[String]

    override def max(values: Any*): String = {
        values
            .map(_.asInstanceOf[String])
            .max
    }

    override def min(values: Any*): String = {
        values
            .map(_.asInstanceOf[String])
            .min
    }

    override def avg(value1: Any, value2: Any): Any = {
        throw new UnsupportedOperationException("This data type does not support averages.")
    }
}

object StringType {
    val encoding: String = "UTF-8"
}
