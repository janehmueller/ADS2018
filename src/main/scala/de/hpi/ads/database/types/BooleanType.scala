package de.hpi.ads.database.types

@SerialVersionUID(107L)
object BooleanType extends DataType {
    override def byteSize = 1

    override def toBytes(data: Any): Array[Byte] = {
        if (data.asInstanceOf[Boolean]) Array(1) else Array(0)
    }

    override def fromBytes(data: Array[Byte]): Boolean = {
        this.testInputLength(data)
        val Array(value) = data
        value == 1
    }

    override def lessThan(a: Any, b: Any): Boolean = a.asInstanceOf[Boolean] < b.asInstanceOf[Boolean]

    override def max(values: Any*): Boolean = {
        values
            .map(_.asInstanceOf[Boolean])
            .exists(identity)
    }

    override def min(values: Any*): Boolean = {
        values
            .map(_.asInstanceOf[Boolean])
            .forall(identity)
    }

    override def avg(value1: Any, value2: Any): Boolean = value1.asInstanceOf[Boolean] || value2.asInstanceOf[Boolean]
}
