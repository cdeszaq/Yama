package yama

import org.jsoup.nodes.Document
import org.jsoup.Jsoup
import org.joda.time.DateTime

import org.joda.time.Minutes
import org.jsoup.nodes.Element

class PageCrawlService {

    static final STALENESS = Minutes.ONE
    static final COOKIES = ["CardDatabaseSettings": "0=1&1=28&2=0&14=1&3=13&4=0&5=1&6=15&7=0&8=1&9=1&10=18&11=7&12=8&15=1&16=0&13=25"]
    static final TIMEOUT = 10000 // 10 second timeout
    static rabbitQueue = "pagesToCrawl"

    /** Add the job to the crawl queue.
     *
     * @param job Job for crawling. type: type of page that will be crawled, url: Url to be crawled.
     */
    void queuePage(Map job) {
        log.trace("Queueing page: ${job.url}")
        rabbitSend rabbitQueue, job
    }

    /** Handle messages coming in from the queue.
     *
     * @param message The crawl job.
     */
    void handleMessage(message) {
        crawlPage(message)
    }

    /** Crawl the page tha the job indicates if the page needs to be crawled.
     *
     * @param job Job for crawling. type: type of page that will be crawled, url: Url to be crawled.
     */
    def crawlPage(Map job) {
        log.trace("Loading page: ${job.url}")
        Page page = Page.findOrCreateByUrlAndPageType(job.url, job.type)

        // Crawl the page if it hasn't been updated in a while
        if (needsCrawling(page)) {
            log.debug("Crawling page: ${page.url}")
            updatePage(page)
            followLinks(page)
        }
    }

    /** Pull the page from the given URL and store it in the database.
     *
     * @param url URL of the page to pull
     */
    def updatePage(Page page) {
        log.trace("Getting page: ${page.url}")
        Document doc = Jsoup.connect(page.url)
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
     * @param page Page to crawl.
     */
    def followLinks(Page page) {
        log.trace("Parseing page: ${page.url}")
        Document doc = Jsoup.parse(page.html, page.url)

        // Select the card printing links
        List<Element> links = doc.select("#ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_otherSetsValue a[href]")
        links.each {queuePage([
                type: PageType.findByName("Card"),
                url: it.attr("abs:href")
        ])}
    }

    /** Determine if we need to pull the page or not.
     *
     * @param page The page under consideration
     * @return True if the page needs to be pulled
     */
    boolean needsCrawling(Page page) {
        !page?.lastUpdated?.isAfter(new DateTime().minus(STALENESS))
    }
}
