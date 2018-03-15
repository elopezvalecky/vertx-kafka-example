package nl.kii.service.content.domain

import nl.kii.xtend.cqrses.ValueObject
import org.eclipse.xtend.lib.annotations.Data

@Data
class TextWeight extends ValueObject {
    val String text
    val double weight
}