package nl.kii.service.content.domain

import java.net.URL
import java.time.Instant
import java.util.UUID
import nl.kii.service.content.event.ArticleAdded
import nl.kii.service.content.event.ArticleDescriptionUpdated
import nl.kii.service.content.event.ArticleTitleUpdated
import org.eclipse.xtend.lib.annotations.Accessors

@Accessors(PUBLIC_GETTER)
class Article extends Item {

    var String title
    var String description
    var URL url

    new(UUID id, String title, String description, URL url, Instant published) {
        apply(new ArticleAdded(id, title, description, url, published))
    }
    
    def update(String title, String description) {
        updateTitle(title)
        updateDescription(description)
        this
    }
    
    def updateTitle(String title) {
        apply(new ArticleTitleUpdated(id, title))
        this
    }
    
    def updateDescription(String description) {
        apply(new ArticleDescriptionUpdated(id, description))
        this
    }

    def private void handle(ArticleAdded event) {
        this.id = event.id
        this.title = event.title
        this.description = event.description
        this.url = event.url
        this.published = event.published
        this.keywords = newArrayList
    }

}