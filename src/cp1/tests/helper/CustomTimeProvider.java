package cp1.tests.helper;

import cp1.base.LocalTimeProvider;

public class CustomTimeProvider implements LocalTimeProvider {

    public static final CustomTimeProvider instance = new CustomTimeProvider();

    private static ThreadLocal<Long> time = new ThreadLocal<>();

    public static void set(long timeValue) {
        time.set(timeValue);
    }

    private CustomTimeProvider() {
    }

    @Override
    public long getTime() {
        return time.get();
    }

}
