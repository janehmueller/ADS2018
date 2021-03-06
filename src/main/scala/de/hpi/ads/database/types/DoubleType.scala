package de.hpi.ads.database.types

@SerialVersionUID(105L)
object DoubleType extends DataType {
    override def byteSize = 8

    override def toBytes(data: Any): Array[Byte] = {
        val longValue = java.lang.Double.doubleToRawLongBits(data.asInstanceOf[Double])
        LongType.toBytes(longValue)
    }

    override def fromBytes(data: Array[Byte]): Double = {
        this.testInputLength(data)
        java.lang.Double.longBitsToDouble(LongType.fromBytes(data))
    }

    override def lessThan(a: Any, b: Any): Boolean = a.asInstanceOf[Double] < b.asInstanceOf[Double]

    override def max(values: Any*): Double = {
        values
            .map(_.asInstanceOf[Double])
            .max
    }

    override def min(values: Any*): Double = {
        values
            .map(_.asInstanceOf[Double])
            .min
    }

    override def avg(value1: Any, value2: Any): Double = (value1.asInstanceOf[Double] + value2.asInstanceOf[Double]) / 2
}
