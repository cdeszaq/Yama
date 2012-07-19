package yama

import org.jsoup.nodes.Document
import org.jsoup.Jsoup
import org.joda.time.DateTime

import org.jsoup.nodes.Element
import grails.converters.JSON

import org.joda.time.Hours
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.transaction.interceptor.TransactionAspectSupport

class PageCrawlService {

    static rabbitQueue = "pagesToCrawl"

    static final STALENESS = Hours.FOUR
    static final COOKIES = ["CardDatabaseSettings": "0=1&1=28&2=0&14=1&3=13&4=0&5=1&6=15&7=0&8=1&9=1&10=18&11=7&12=8&15=1&16=0&13="]
    static final TIMEOUT = 30000 // 30 second timeout

    /** Add the job to the crawl queue.
     *
     * @param job Job for crawling. type: type of page that will be crawled, url: Url to be crawled.
     */
    void queuePage(Map job) {
        log.trace("Queueing page: ${job.url}")
        rabbitSend rabbitQueue, (job as JSON).toString()
    }

    /** Queue the given URL as a card if it needs crawling.
     *
     * @param url Url to queue if we need to.
     */
    void queueCardIfNeeded(String url) {
        if (needsCrawling(url)) {
            queuePage([
                    type: PageType.findOrSaveByName("Card").name,
                    url: url
            ])
        }
    }

    /** Queue the given URL as a set if it needs crawling.
     *
     * @param url Url to queue if we need to.
     */
    void queueSetIfNeeded(String url) {
        if (needsCrawling(url)) {
            queuePage([
                    type: PageType.findOrSaveByName("Set").name,
                    url: url
            ])
        }
    }

    /** Handle messages coming in from the queue.
     *
     * @param message The crawl job.
     */
    void handleMessage(message) {
        // Build the job map back from the message
        def parsedMessage = JSON.parse(message.toString())
        Map job = [type: PageType.findByName(parsedMessage.type), url: parsedMessage.url]

        // Make sure we could find a page type
        if (job.type != null) {
            crawlPage(job)
        } else {
            // We don't know about that page type, so put the job back in the queue
            // Note: Could happen if the bootstrap has not fired to populate the DB
            log.warn("Unkown page type: ${parsedMessage.type}")
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly()
        }
    }

    /** Crawl the page tha the job indicates if the page needs to be crawled.
     *
     * @param job Job for crawling. type: type of page that will be crawled, url: Url to be crawled.
     */
    def crawlPage(Map job) {
        log.trace("Loading page: ${job.url}")
        Page page = Page.findOrCreateByUrlAndPageType(job.url, job.type)
        page.pageType = job.type // Need to set manually b/c of http://jira.grails.org/browse/GRAILS-9272

        // Crawl the page if it hasn't been updated in a while
        if (needsCrawling(page)) {
            log.debug("Crawling page: ${page.url}")
            updatePage(page)
            followLinks(page)
        }
    }

    /** Pull the page and store it in the database.
     *
     * @param page The page to pull
     */
    def updatePage(Page page) {
        log.trace("Getting page: ${page.url}")
        Document doc = Jsoup.connect(cleanUrl(page.url))
                .timeout(TIMEOUT)
                .cookies(COOKIES)
                .get()

        log.trace("Updating page: ${page.url}")
        page.html = doc.outerHtml()
        page.lastUpdated = new DateTime() // Manually set the updated time to force a save. Since the framework is
        // controlling this field, any value we set here is corrected to reality anyways so this is essentially the same
        // as marking it dirty.

        log.trace("Saving page: ${page.url}")
        if(!page.save()) {
            log.error("Unable to save page: ${page.url}")
        }
    }

    /** Crawl the HTML of the given page and add interesting links to the crawl queue.
     *
     * @param page Page to crawl
     */
    def followLinks(Page page) {
        log.trace("Parseing page: ${page.url}")
        Document doc = Jsoup.parse(page.html, page.url.toString())

        // Follow links based on page type
        switch (page.pageType) {
            case PageType.findOrSaveByName("Card"):
                followCardPageLinks(doc)
                break
            case PageType.findOrSaveByName("Set"):
                followSetPageLinks(doc)
                break
            default:
                log.warn("Unknown page type.")
        }
    }

    /** Add interesting links in a Card page to the crawl queue.
     *
     * @param doc Document to investigate
     */
    def followCardPageLinks(Document doc) {
        log.trace("Following card page links on: ${doc.baseUri()}")
        // Queue the card printing links
        List<Element> cardLinks = doc.select("#ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_otherSetsValue a[href]")
        cardLinks.each { queueCardIfNeeded(it.attr("abs:href")) }

        // Queue the set link for single and for flip cards
        Element setLink = doc.select("#ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_currentSetSymbol a[href]")[1]
        setLink = setLink ?: doc.select("#ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_ctl05_currentSetSymbol a[href]")[1]
        queueSetIfNeeded(setLink.attr("abs:href"))
    }

    /** Add interesting links in a Set page to the crawl queue.
     *
     * @param doc Document to investigate
     */
    def followSetPageLinks(Document doc) {
        log.trace("Following set page links on: ${doc.baseUri()}")
        // Queue the card links
        List<Element> cardLinks = doc.select("a[href].nameLink")
        cardLinks.each { queueCardIfNeeded(it.attr("abs:href")) }
    }

    /** Determine if we need to crawl the page or not.
     *
     * @param page The page under consideration
     * @return True if the page needs to be crawled
     */
    boolean needsCrawling(Page page) {
        !page?.lastUpdated?.isAfter(new DateTime().minus(STALENESS))
    }

    /** Convenience method for needsCrawling(Page).
     *
     * @param url
     * @return
     */
    boolean needsCrawling(URL url) {
        needsCrawling(Page.findByUrl(url))
    }

    /** Convenience method for needsCrawling(Page).
     *
     * @param url
     * @return
     */
    boolean needsCrawling(String url) {
        needsCrawling(new URL(url))
    }

    /** Generate a clean and correct URI from the given URL string.
     *
     * @param url URL string to clean up
     * @return A nice, clean, valid URL
     */
    String cleanUrl(String url) {
        URL cleanUrl = new URL(URLDecoder.decode(url))
        new URI(cleanUrl.protocol, cleanUrl.userInfo, cleanUrl.host, cleanUrl.port, cleanUrl.path, cleanUrl.query, null).toString()
    }

    /** Clean up a URL that is a URL and not a string
     *
     * @param url URL to clean
     * @return A nice, clean, valid URL
     */
    String cleanUrl(URL url) {
        cleanUrl(url.toString())
    }
}
