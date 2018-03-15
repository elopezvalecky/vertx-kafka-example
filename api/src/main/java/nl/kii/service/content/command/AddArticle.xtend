package nl.kii.service.content.command

import io.vertx.codegen.annotations.DataObject
import io.vertx.core.json.JsonObject
import java.net.URL
import java.time.Instant
import java.util.UUID
import nl.kii.xtend.cqrses.Command
import org.eclipse.xtend.lib.annotations.Data
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

import static extension java.util.UUID.fromString

@Data
@DataObject
@FinalFieldsConstructor
class AddArticle implements Command {
    
    val UUID id
    val String title
    val String description
    val Instant published
    val URL url

    new(JsonObject json) {
        this.id = json.getString('id').fromString
        this.title = json.getString('title')
        this.description = json.getString('description')
        this.published = json.getInstant('published')
        this.url = new URL(json.getString('url'))
    }

    def toJson() {
        new JsonObject => [
            put('id', this.id.toString)
            put('title', this.title)
            put('description', this.description)
            put('published', this.published)
            put('url', this.url.toString)
        ]
    }

}