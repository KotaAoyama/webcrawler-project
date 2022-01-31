package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {

    private final Clock clock;
    private final ProfilingState state;
    private final Object delegate;
    private ZonedDateTime startTime;

    ProfilingMethodInterceptor(Clock clock, ZonedDateTime startTime, ProfilingState state, Object delegate) {
        this.clock = Objects.requireNonNull(clock);
        this.startTime = Objects.requireNonNull(startTime);
        this.state = Objects.requireNonNull(state);
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {

        Object result = null;
        try {
            result = method.invoke(delegate, args);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e.getCause().getMessage());
        } finally {
            Profiled annotation = method.getAnnotation(Profiled.class);
            if (annotation != null) {
                ZonedDateTime now = ZonedDateTime.now(clock);
                Duration duration = Duration.between(startTime, now);
                startTime = now;
                state.record(delegate.getClass(), method, duration);
            }
        }
        return result;
    }
}
