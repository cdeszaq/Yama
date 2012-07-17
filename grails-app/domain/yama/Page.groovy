package yama

import org.joda.time.DateTime

class Page {

    DateTime dateCreated
    DateTime lastUpdated

    String url
    String html

    static constraints = {
        url blank: false, unique: true
        html blank: false
    }

    static mapping = {
        html type: 'text'
    }
}
