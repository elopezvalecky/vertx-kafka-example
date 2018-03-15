package nl.kii.service.content.command

import io.vertx.codegen.annotations.DataObject
import io.vertx.core.json.JsonObject
import java.util.UUID
import nl.kii.xtend.cqrses.Command
import org.eclipse.xtend.lib.annotations.Data
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

import static extension java.util.UUID.fromString

@Data
@DataObject
@FinalFieldsConstructor
class UpdateArticle implements Command {
    
    val UUID id
    val String title
    val String description

    new(JsonObject json) {
        this.id = json.getString('id').fromString
        this.title = json.getString('title')
        this.description = json.getString('description')
    }

    def toJson() {
        new JsonObject => [
            put('id', id.toString)
            put('title', this.title)
            put('description', this.description)
        ]
    }    
}