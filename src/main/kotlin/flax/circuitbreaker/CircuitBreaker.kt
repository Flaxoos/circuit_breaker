package flax.circuitbreaker

import flax.circuitbreaker.CircuitBreaker.CircuitBreakerState.CLOSED
import flax.circuitbreaker.CircuitBreaker.CircuitBreakerState.HALF_OPEN
import flax.circuitbreaker.CircuitBreaker.CircuitBreakerState.OPEN
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class CircuitBreaker(
    private val failureThreshold: Long = 3,
    private val halfOpenAttempts: Long = 2,
    private val timeout: Duration = 1.seconds
) {
    private var errorCounter: AtomicLong = AtomicLong(0)
    private var state = CLOSED
    private val stateLock = ReentrantReadWriteLock()
    private val openCircuitBreakerException = OpenCircuitBreakerException("Action failed more than $failureThreshold times, subsequent calls will be prevented until action is successful again")
    private val halfOpenCircuitBreakerException = HalfOpenCircuitBreakerException("Action still failing after timeout period of $timeout, subsequent calls will be prevented until action is successful again")

    suspend fun <T> guard(action: suspend () -> T): T {
        return when (stateLock.read { state }) {
            CLOSED ->
                attemptAction(failureThreshold) {
                    action()
                }
            OPEN ->
                throw openCircuitBreakerException
            HALF_OPEN ->
                return try {
                    attemptAction(halfOpenAttempts) {
                        action()
                    }.also {
                        state = CLOSED
                    }
                } catch (e: Exception) {
                    throw if (e is OpenCircuitBreakerException)
                        e
                    else
                        halfOpenCircuitBreakerException
                }
        }
    }

    private suspend fun <T> attemptAction(threshold: Long, action: suspend () -> T) =
        if (errorCounter.get() < threshold) {
            try {
                action().also { errorCounter.set(0) }
            } catch (e: Exception) {
                errorCounter.incrementAndGet()
                throw e
            }
        } else {
            openCircuit()
            throw openCircuitBreakerException
        }

    private suspend fun openCircuit() {
        stateLock.write { state = OPEN }
        errorCounter.set(0)
        CoroutineScope(coroutineContext).launch {
            delay(timeout)
            stateLock.write { state = HALF_OPEN }
        }
    }

    private enum class CircuitBreakerState {
        CLOSED, OPEN, HALF_OPEN
    }
}

sealed class CircuitBreakerException(override val message: String?) : Exception(message)
class OpenCircuitBreakerException(override val message: String?) : CircuitBreakerException(message)
class HalfOpenCircuitBreakerException(override val message: String?) : CircuitBreakerException(message)
