package com.example.validator.util;

import java.time.Duration;
import java.util.function.Supplier;

public class RetryUtils {

    private RetryUtils() {
    }

    public static <T> T retryUntilSuccess(

            Supplier<T> supplier,

            Duration timeout,

            Duration interval

    ) {

        long deadline =
                System.currentTimeMillis()
                        + timeout.toMillis();

        RuntimeException lastException = null;

        while (System.currentTimeMillis() < deadline) {

            try {

                T result = supplier.get();

                if (result != null) {
                    return result;
                }

            } catch (RuntimeException ex) {

                lastException = ex;

            }

            try {

                Thread.sleep(interval.toMillis());

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();

                throw new RuntimeException(e);

            }

        }

        if (lastException != null) {
            throw lastException;
        }

        throw new RuntimeException(
                "Operation timed out after "
                        + timeout.toSeconds()
                        + " seconds."
        );

    }

}