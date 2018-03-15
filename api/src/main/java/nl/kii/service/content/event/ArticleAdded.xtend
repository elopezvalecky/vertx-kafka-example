package nl.kii.service.content.event

import java.net.URL
import java.time.Instant
import java.util.UUID
import nl.kii.xtend.cqrses.Event
import org.eclipse.xtend.lib.annotations.Accessors

@Accessors(PUBLIC_GETTER)
class ArticleAdded implements Event {
    
    var UUID id
    var String title
    var String description
    var URL url
    var Instant published

    private new() {}
    new(UUID id, String title, String description, URL url, Instant published) {
        this.id = id
        this.title = title
        this.description = description
        this.url = url
        this.published = published
    }

}