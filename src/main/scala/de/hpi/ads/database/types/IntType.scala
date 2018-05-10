package de.hpi.ads.database.types

import java.nio.ByteBuffer

import boopickle.Default._

import scala.reflect.runtime.universe._

object IntType extends DataType {
    type InternalType = Int
    val tag: TypeTag[InternalType] = typeTag[InternalType]

    def fromBytes(binaryData: ByteBuffer): Any = Unpickle[Int].fromBytes(binaryData)
}
