package de.hpi.ads.database.types

import java.nio.ByteBuffer

import scala.reflect.runtime.universe._
import boopickle.Default._

object DoubleType extends DataType {
    type InternalType = Double
    val tag: TypeTag[InternalType] = typeTag[InternalType]

    def fromBytes(binaryData: ByteBuffer): Any = Unpickle[Double].fromBytes(binaryData)
}
