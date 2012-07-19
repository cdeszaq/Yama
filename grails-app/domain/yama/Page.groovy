package yama

import org.joda.time.DateTime

class Page {

    DateTime dateCreated
    DateTime lastUpdated

    PageType pageType
    URL url
    String html

    static constraints = {
        url unique: true
        html blank: false
    }

    static mapping = {
        html type: 'text'
    }
}
