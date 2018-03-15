package nl.kii.service.content.event

import java.util.UUID
import nl.kii.xtend.cqrses.Event
import org.eclipse.xtend.lib.annotations.Accessors

@Accessors(PUBLIC_GETTER)
class ArticleTitleUpdated implements Event {

    var UUID id
    var String title

    private new() {}
    new(UUID id, String title) {
        this.title = title
    }

}
