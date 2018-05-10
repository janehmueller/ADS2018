package de.hpi.ads.database.types

import java.nio.ByteBuffer

import scala.reflect.runtime.universe._

object BinaryType extends DataType {
    type InternalType = Array[Byte]
    val tag: TypeTag[InternalType] = typeTag[InternalType]

    def fromBytes(binaryData: ByteBuffer): Any = binaryData.array()

    override def toBytes(data: Any): Array[Byte] = data.asInstanceOf[InternalType]
}
