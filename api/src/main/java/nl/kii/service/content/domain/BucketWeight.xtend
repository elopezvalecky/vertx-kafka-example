package nl.kii.service.content.domain

import java.util.UUID
import nl.kii.xtend.cqrses.ValueObject
import org.eclipse.xtend.lib.annotations.Data

@Data
class BucketWeight extends ValueObject {
    val UUID bucketId
    val double weight
}