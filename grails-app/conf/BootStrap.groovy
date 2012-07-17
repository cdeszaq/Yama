import yama.PageType

class BootStrap {

    def init = { servletContext ->
        List baseline = []

        baseline << new PageType(name: "Card")
        baseline << new PageType(name: "Set")

        baseline.each {it.save()}
    }
    def destroy = {
    }
}
