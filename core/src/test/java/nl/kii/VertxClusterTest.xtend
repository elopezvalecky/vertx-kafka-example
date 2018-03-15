package nl.kii

import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus
import io.vertx.core.net.impl.ServerID
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(VertxUnitRunner)
class VertxClusterTest {

    val Supplier<Vertx> supplier = [
        val latch = new CountDownLatch(1)
        val vertx = new AtomicReference<Vertx>
        Vertx.clusteredVertx(new VertxOptions()) [
            if(failed) throw new RuntimeException('Unable to create clustered Vertx')
            vertx.set(result)
            latch.countDown
        ]
        try {
            latch.await
        } catch (InterruptedException e) {
            throw new RuntimeException(e)
        }
        vertx.get
    ]

    @Rule public val rule = new RunTestOnContext(supplier)


    @Test def void getServerID(TestContext ctx) {
        val serverIDField  = ClusteredEventBus.getDeclaredField('serverID') => [
            accessible = true
        ]
        val serverID = serverIDField.get(rule.vertx.eventBus) as ServerID
        println(serverID)
        ctx.assertNotNull(serverID)
    }
}