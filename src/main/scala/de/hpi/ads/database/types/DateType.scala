package de.hpi.ads.database.types

import java.util.Date

object DateType extends DataType {
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
}
