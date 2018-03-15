package nl.kii.eventstore.v2

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import io.vertx.core.eventbus.impl.codecs.ByteArrayMessageCodec
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import nl.kii.eventsourcing.AggregateRoot
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream

@FinalFieldsConstructor
class AggregateCodec<AR extends AggregateRoot> implements MessageCodec<AR, AR> {
    
    val codec = new ByteArrayMessageCodec
    
    val Class<AR> type
    
    override decodeFromWire(int pos, Buffer buffer) {
        val bais = new ByteArrayInputStream(codec.decodeFromWire(pos, buffer))
        val in = new ObjectInputStream(bais)
        type.cast(in.readObject) => [
            in.close
            bais.close
        ]            
    }
    
    override encodeToWire(Buffer buffer, AR s) {
        val baos = new ByteArrayOutputStream
        val out = new ObjectOutputStream(baos)
        out.writeObject(s)
        codec.encodeToWire(buffer, baos.toByteArray)
        out.close
        baos.close
    }
    
    override name() {
        type.name
    }
    
    override systemCodecID() {
        (-1).byteValue
    }
    
    override transform(AR s) {
        s
    }
    
}