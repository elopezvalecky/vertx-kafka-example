package nl.kii.service.content.event

import java.util.UUID
import nl.kii.xtend.cqrses.Event
import org.eclipse.xtend.lib.annotations.Accessors

@Accessors(PUBLIC_GETTER)
class ArticleDescriptionUpdated implements Event {
    
    var UUID id
    var String description
    
    private new() {}
    new(UUID id, String description) {
        this.description = description
    }
    
}