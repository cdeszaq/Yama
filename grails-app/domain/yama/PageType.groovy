package yama

import org.joda.time.DateTime

class PageType implements Serializable {

    DateTime dateCreated
    DateTime lastUpdated

    String name

    static hasMany = [pages: Page]

    static constraints = {
        name blank: false, unique: true
    }
}
