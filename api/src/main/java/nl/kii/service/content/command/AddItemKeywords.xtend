package nl.kii.service.content.command

import io.vertx.codegen.annotations.DataObject
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.util.List
import java.util.UUID
import nl.kii.service.content.domain.TextWeight
import nl.kii.xtend.cqrses.Command
import org.eclipse.xtend.lib.annotations.Data
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

import static extension java.util.UUID.fromString

@Data
@DataObject
@FinalFieldsConstructor
class AddItemKeywords implements Command {
    
    val UUID id
    val List<TextWeight> keywords

    new(JsonObject json) {
        this.id = json.getString('id').fromString
        this.keywords = json.getJsonArray('keywords').list
            .map[
                val pair = it as JsonObject
                new TextWeight(pair.getString('text'), pair.getDouble('weight'))
            ]
    }

    def toJson() {
        new JsonObject => [
            put('id', id.toString)
            put('keywords', new JsonArray => [ list|
                keywords.forEach[ pair|
                    list.add(new JsonObject() => [
                        put('text', pair.text)
                        put('weight', pair.weight)
                    ])
                ]
            ])
        ]
    }        
}