/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ads.database

import java.util.Date

package object operators {
    trait Operator {
        def column: String
        def value: Any

        def compare(other: Any): Boolean

        def compareAny(other: Any): Boolean = {
            // TODO: support comparisons like < for different data types (e.g., int and long)
            other.getClass == value.getClass && this.compare(other)
        }

        def apply(row: Row): Boolean = this.compareAny(row.getByName(column))

        def apply(other: Any): Boolean = this.compareAny(other)

        def apply(index: Map[Int, Any]): Map[Int, Any] = {
            // TODO filter index and return indexed values that return true for the operator
            throw new NotImplementedError()
        }
    }

    case class EqOperator(column: String, value: Any) extends Operator {
        override def compare(other: Any): Boolean = other == value
        override def compareAny(other: Any): Boolean = other == value
    }

    case class NeqOperator(column: String, value: Any) extends Operator {
        override def compare(other: Any): Boolean = other != value
        override def compareAny(other: Any): Boolean = other != value
    }

    case class LessThanOperator(column: String, value: Any) extends Operator {
        override def compare(other: Any): Boolean = other match {
            case x: Boolean => x < value.asInstanceOf[Boolean]
            case x: Date => x.getTime < value.asInstanceOf[Date].getTime
            case x: Double => x < value.asInstanceOf[Double]
            case x: Int => x < value.asInstanceOf[Int]
            case x: Long => x < value.asInstanceOf[Long]
            case x: String => x < value.asInstanceOf[String]
            case _ => false
        }
    }

    case class LessThanEqOperator(column: String, value: Any) extends Operator {
        override def compare(other: Any): Boolean = other match {
            case x: Boolean => x <= value.asInstanceOf[Boolean]
            case x: Date => x.getTime <= value.asInstanceOf[Date].getTime
            case x: Double => x <= value.asInstanceOf[Double]
            case x: Int => x <= value.asInstanceOf[Int]
            case x: Long => x <= value.asInstanceOf[Long]
            case x: String => x <= value.asInstanceOf[String]
            case _ => false
        }
    }

    case class GreaterThanOperator(column: String, value: Any) extends Operator {
        override def compare(other: Any): Boolean = other match {
            case x: Boolean => x > value.asInstanceOf[Boolean]
            case x: Date => x.getTime > value.asInstanceOf[Date].getTime
            case x: Double => x > value.asInstanceOf[Double]
            case x: Int => x > value.asInstanceOf[Int]
            case x: Long => x > value.asInstanceOf[Long]
            case x: String => x > value.asInstanceOf[String]
            case _ => false
        }
    }

    case class GreaterThanEqOperator(column: String, value: Any) extends Operator {
        override def compare(other: Any): Boolean = other match {
            case x: Boolean => x >= value.asInstanceOf[Boolean]
            case x: Date => x.getTime >= value.asInstanceOf[Date].getTime
            case x: Double => x >= value.asInstanceOf[Double]
            case x: Int => x >= value.asInstanceOf[Int]
            case x: Long => x >= value.asInstanceOf[Long]
            case x: String => x >= value.asInstanceOf[String]
            case _ => false
        }
    }
}
