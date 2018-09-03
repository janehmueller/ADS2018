package de.hpi.ads.database.types

import java.util.Date

@SerialVersionUID(101L)
object DateType extends DataType with Serializable {
    override def byteSize = 8

    override def toBytes(data: Any): Array[Byte] = {
        val longValue = data.asInstanceOf[Date].getTime
        LongType.toBytes(longValue)
    }

    override def fromBytes(data: Array[Byte]): Date = {
        this.testInputLength(data)
        new Date(LongType.fromBytes(data))
    }

    override def lessThan(a: Any, b: Any): Boolean = {
        a.asInstanceOf[Date].getTime < b.asInstanceOf[Date].getTime
    }

    override def max(values: Any*): Date = {
        val maxValue = values
            .map(_.asInstanceOf[Date])
            .map(_.getTime)
            .max
        new Date(maxValue)
    }

    override def min(values: Any*): Date = {
        val minValue = values
            .map(_.asInstanceOf[Date])
            .map(_.getTime)
            .min
        new Date(minValue)
    }

    override def avg(value1: Any, value2: Any): Any = {
        throw new UnsupportedOperationException("This data type does not support averages.")
    }
}
