package flax.circuitbreaker

import io.kotest.assertions.throwables.shouldNotThrow
import io.kotest.assertions.throwables.shouldThrow
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.properties.Delegates
import kotlin.time.Duration.Companion.seconds

internal class CircuitBreakerTest {

    private val failureThreshold = 3L
    private val halfOpenAttempts = 2L
    private val timeout = 1.seconds

    private var circuitBreaker by Delegates.notNull<CircuitBreaker>()
    private var helloSayer by Delegates.notNull<HelloSayer>()

    @BeforeEach
    fun init() {
        circuitBreaker = CircuitBreaker(
            failureThreshold = failureThreshold,
            halfOpenAttempts = halfOpenAttempts,
            timeout = timeout
        )
        helloSayer = HelloSayer()
    }

    @Test
    fun `should let actions through when open`() {
        runBlocking {
            shouldNotThrow<Exception> { circuitBreaker.guard { helloSayer.sayHello() } }
        }
    }

    @Test
    fun `should switch to open after failure threshold`() {
        helloSayer.working = false
        runBlocking {
            repeat(failureThreshold.toInt()) {
                shouldThrow<IllegalStateException> {
                    circuitBreaker.guard { helloSayer.sayHello() }
                }
            }

            shouldThrow<OpenCircuitBreakerException> {
                circuitBreaker.guard { helloSayer.sayHello() }
            }
        }
    }

    @Test
    fun `should switch to half open after failure threshold exceeded and timeout period passed`() {
        helloSayer.working = false
        runBlocking {
            repeat(failureThreshold.toInt()) {
                shouldThrow<IllegalStateException> {
                    circuitBreaker.guard { helloSayer.sayHello() }
                }
            }
            shouldThrow<OpenCircuitBreakerException> {
                circuitBreaker.guard { helloSayer.sayHello() }
            }

            delay(timeout * 1.1)

            shouldThrow<HalfOpenCircuitBreakerException> {
                circuitBreaker.guard { helloSayer.sayHello() }
            }
        }
    }

    @Test
    fun `should switch to open after failure threshold exceeded and timeout period passed and half open attempts exceeded`() {
        helloSayer.working = false
        runBlocking {
            repeat(failureThreshold.toInt()) {
                shouldThrow<IllegalStateException> {
                    circuitBreaker.guard { helloSayer.sayHello() }
                }
            }
            shouldThrow<OpenCircuitBreakerException> {
                circuitBreaker.guard { helloSayer.sayHello() }
            }

            delay(timeout * 1.1)

            repeat(halfOpenAttempts.toInt()) {
                shouldThrow<HalfOpenCircuitBreakerException> {
                    circuitBreaker.guard { helloSayer.sayHello() }
                }
            }

            shouldThrow<OpenCircuitBreakerException> {
                circuitBreaker.guard { helloSayer.sayHello() }
            }
        }
    }

    @Test
    fun `should switch to closed after failure threshold exceeded and timeout period passed and action works again`() {
        helloSayer.working = false
        runBlocking {
            repeat(failureThreshold.toInt()) {
                shouldThrow<IllegalStateException> {
                    circuitBreaker.guard { helloSayer.sayHello() }
                }
            }
            shouldThrow<OpenCircuitBreakerException> {
                circuitBreaker.guard { helloSayer.sayHello() }
            }

            delay(timeout * 1.1)

            helloSayer.working = true

            shouldNotThrow<Exception> {
                circuitBreaker.guard { helloSayer.sayHello() }
            }
        }
    }

    class HelloSayer {
        var working: Boolean = true
        fun sayHello() =
            if (working)
                "hello"
            else
                throw IllegalStateException("GoodBye")
    }
}
