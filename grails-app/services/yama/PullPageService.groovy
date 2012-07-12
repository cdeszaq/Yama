package yama

import org.jsoup.nodes.Document
import org.jsoup.Jsoup

class PullPageService {

    static rabbitQueue = "pagesToPull"

    void queuePagePull (String url) {
        rabbitSend rabbitQueue, url
    }

    void handleMessage(message) {
        // The message is just the URL
        pullPage(message)
    }

    /** Pull the page from the given URL and store it in the database.
     *
     * @param url URL of the page to pull
     */
    def pullPage(String url) {
        // Get the page
        log.debug("Getting page: $url")
        Document doc = Jsoup.connect(url).get()

        // Get the existing page of make a new one and update the HTML
        log.trace("Updating page: $url")
        Page pageInstance = Page.findOrCreateByUrl(url)
        pageInstance.html = doc.outerHtml()

        // Save the page
        log.trace("Saving page: $url")
        if(!pageInstance.save(flush: true)) {
            log.error("Unable to save page: $url")
        }
    }
}
