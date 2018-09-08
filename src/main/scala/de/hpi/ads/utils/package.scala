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

package de.hpi.ads

package object utils {

    def medianUpTo5(five: Array[Any], lt: (Any, Any) => Boolean): Any = {
        def order2(a: Array[Any], i: Int, j: Int, lt: (Any, Any) => Boolean): Unit = {
            if (lt(a(j), a(i))) {
                val t = a(i)
                a(i) = a(j)
                a(j) = t
            }
        }

        def pairs(a: Array[Any], i: Int, j: Int, k: Int, l: Int, lt: (Any, Any) => Boolean) = {
            if (lt(a(i), a(k))) {
                order2(a, j, k, lt)
                a(j)
            } else {
                order2(a, i, l, lt)
                a(i)
            }
        }

        if (five.length < 2) {
            return five(0)
        }
        order2(five, 0, 1, lt)
        if (five.length < 4) {
            return (
                if (five.length == 2 || lt(five(2), five(0))){
                    five(0)
                } else if (lt(five(1), five(2))) {
                    five(1)
                } else {
                    five(2)
                }
                )
        }
        order2(five, 2, 3, lt)
        if (five.length < 5){
            pairs(five, 0, 1, 2, 3, lt)
        } else if (lt(five(0), five(2))) {
            order2(five, 1, 4, lt)
            pairs(five, 1, 4, 2, 3, lt)
        } else {
            order2(five, 3, 4, lt)
            pairs(five, 0, 1, 3, 4, lt)
        }
    }

    def medianOfMedians(arr: Array[Any], lt: (Any, Any) => Boolean): Any = {
        val medians = arr
            .grouped(5)
            .map(x => medianUpTo5(x, lt))
            .toArray
        if (medians.length <= 5) {
            medianUpTo5(medians, lt)
        } else {
            medianOfMedians(medians, lt)
        }
    }

    def maxOf(arr: Array[Any], lt: (Any, Any) => Boolean): Any = {
        var curMax = arr(0)
        for (i <- 1 until arr.length) {
            if (lt(curMax, arr(i))) {
                curMax = arr(i)
            }
        }
        curMax
    }
}
