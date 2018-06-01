package de.hpi.ads.database.types

object IntType extends DataType {
    override def byteSize = 4

    override def toBytes(data: Any): Array[Byte] = {
        val intValue = data.asInstanceOf[Int]
        Array(
            intValue >> 24,
            intValue >> 16,
            intValue >> 8,
            intValue >> 0
        ).map(_ & 0xff)
        .map(_.asInstanceOf[Byte])
    }

    override def fromBytes(data: Array[Byte]): Int = {
        this.testInputLength(data)
        val intData = data.map(0xff & _)
        intData(0) << 24  |
            intData(1) << 16  |
            intData(2) << 8   |
            intData(3) << 0
    }

    override def lessThan(a: Any, b: Any): Boolean = a.asInstanceOf[Int] < b.asInstanceOf[Int]

    override def max(values: Any*): Int = {
        values
            .map(_.asInstanceOf[Int])
            .max
    }

    override def min(values: Any*): Int = {
        values
            .map(_.asInstanceOf[Int])
            .min
    }
}
