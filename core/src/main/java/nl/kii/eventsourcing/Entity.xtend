package nl.kii.eventsourcing

import java.io.Serializable
import java.util.UUID
import org.eclipse.xtend.lib.annotations.Accessors

abstract class Entity implements Serializable {

    @Accessors( PUBLIC_GETTER, PROTECTED_SETTER ) 
    var UUID id

}