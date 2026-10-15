package com.electronik54.producer.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

@Slf4j
@Aspect
@Component
public class LoggingAspect {

//    private static final Logger log = LoggerFactory.getLogger(LoggingAspect.class);

        @Around("execution(* com.electronik54.producer.controller..*(..))"
        +"|| execution(* com.electronik54.producer.service..*(..))")
    public Object logExecution(ProceedingJoinPoint joinPoint) throws Throwable {
        String method = joinPoint.getSignature().toShortString();
        long start = System.nanoTime();
        log.info("-> {}", method);
        try {
            Object result = joinPoint.proceed();
            log.info("<- {} ({} ms)", method, elapsedMs(start));
            return result;
        } catch (Throwable ex) {
            log.error("!! {} threw {} after {} ms", method, ex.getClass().getSimpleName(), elapsedMs(start));
            throw ex;
        }
    }

    private long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

}
