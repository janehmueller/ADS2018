package de.hpi.ads.database.types

object LongType extends DataType {
    override def byteSize = 8

    override def toBytes(data: Any): Array[Byte] = {
        val longValue = data.asInstanceOf[Long]
        Array(
            longValue >> 56,
            longValue >> 48,
            longValue >> 40,
            longValue >> 32,
            longValue >> 24,
            longValue >> 16,
            longValue >> 8,
            longValue >> 0
        ).map(_ & 0xff)
        .map(_.asInstanceOf[Byte])
    }

    override def fromBytes(data: Array[Byte]): Long = {
        this.testInputLength(data)
        val longData = data
            .map(0xff & _)
            .map(_.asInstanceOf[Long])
        longData(0) << 56  |
            longData(1) << 48  |
            longData(2) << 40  |
            longData(3) << 32  |
            longData(4) << 24  |
            longData(5) << 16  |
            longData(6) << 8   |
            longData(7) << 0
    }

    override def lessThan(a: Any, b: Any): Boolean = a.asInstanceOf[Long] < b.asInstanceOf[Long]

    override def max(values: Any*): Long = {
        values
            .map(_.asInstanceOf[Long])
            .max
    }

    override def min(values: Any*): Long = {
        values
            .map(_.asInstanceOf[Long])
            .min
    }
}
