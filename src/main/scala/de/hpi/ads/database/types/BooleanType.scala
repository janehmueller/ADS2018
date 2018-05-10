package de.hpi.ads.database.types

import java.nio.ByteBuffer

import scala.reflect.runtime.universe._
import boopickle.Default._

object BooleanType extends DataType {
    type InternalType = Boolean
    val tag: TypeTag[InternalType] = typeTag[InternalType]

    def fromBytes(binaryData: ByteBuffer): Any = Unpickle[Boolean].fromBytes(binaryData)
}
