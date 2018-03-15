package nl.kii.service.content.command

import java.net.URL
import java.util.UUID
import nl.kii.xtend.cqrses.Command
import org.eclipse.xtend.lib.annotations.Accessors
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import io.vertx.codegen.annotations.DataObject
import io.vertx.core.json.JsonObject
import static extension java.util.UUID.fromString

@FinalFieldsConstructor
@Accessors
@DataObject
class AddRssCommand implements Command {
    
    val UUID id
    val String title
    val URL url

    new(JsonObject json) {
        this.id = json.getString('id').fromString
        this.title = json.getString('title')
        this.url = new URL(json.getString('url'))
    }    

    def toJson() {
        new JsonObject().put('id', this.id.toString)
            .put('title', this.title)
            .put('url', this.url.toString)
    }    

}

@FinalFieldsConstructor
@Accessors
@DataObject
class RemoveRssCommand implements Command {
    
    val UUID id

    new(JsonObject json) {
        this.id = json.getString('id').fromString
    }
    
    def toJson() {
        new JsonObject().put('id', this.id.toString)
    }

}