package de.hpi.ads.database.types

import java.nio.ByteBuffer

import boopickle.Default._

import scala.reflect.runtime.universe._

trait DataType {
    type InternalType
    val tag: TypeTag[InternalType]

    def fromBytes(binaryData: ByteBuffer): Any

    def toBytes(data: Any): Array[Byte] = Pickle.intoBytes(data).array
}
