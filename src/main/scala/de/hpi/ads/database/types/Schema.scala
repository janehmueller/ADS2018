package de.hpi.ads.database.types

class Schema(schemaString: String) {
    val attributes: List[String] = schemaString.split(";").toList

    val numValues: Int = attributes.length

    val keyIndex = 0

    val key: String = attributes(keyIndex)

    def indexOfAttribute(attribute: String): Int = attributes.indexOf(attribute)
}
