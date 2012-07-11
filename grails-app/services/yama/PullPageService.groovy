package yama

import org.jsoup.nodes.Document
import org.jsoup.Jsoup

class PullPageService {

    /** Pull the page from the given URL and store it in the database.
     *
     * @param url URL of the page to pull
     */
    def pullPage(String url) {
        // Get the page
        log.debug("Getting page: $url")
        Document doc = Jsoup.connect(url).get()

        // Save the page
        log.trace("Saving page: $url")
        def pageInstance = new Page(url: url, html: doc.outerHtml())
        if(!pageInstance.save(flush: true)) {
            log.error("Unable to save page: $url")
        }
    }
}
