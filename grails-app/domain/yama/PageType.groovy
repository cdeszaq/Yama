package yama

import org.joda.time.DateTime

class PageType implements Serializable {

    DateTime dateCreated
    DateTime lastUpdated

    String name

    static hasMany = [page: Page]

    static constraints = {
        name blank: false, unique: true
    }
}
