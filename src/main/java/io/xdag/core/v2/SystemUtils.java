package io.xdag.core.v2;

import lombok.extern.slf4j.Slf4j;
import oshi.SystemInfo;

import java.util.Locale;

@Slf4j
public class SystemUtils {

    static {
        System.setProperty("jna.nosys", "true");
    }

    public static class Code {
        // success
        public static final int OK = 0;

        // wallet
        public static final int FAILED_TO_WRITE_WALLET_FILE = 11;
        public static final int FAILED_TO_UNLOCK_WALLET = 12;
        public static final int ACCOUNT_NOT_EXIST = 13;
        public static final int ACCOUNT_ALREADY_EXISTS = 14;
        public static final int INVALID_PRIVATE_KEY = 15;
        public static final int WALLET_LOCKED = 16;
        public static final int PASSWORD_REPEAT_NOT_MATCH = 17;
        public static final int WALLET_ALREADY_EXISTS = 18;
        public static final int WALLET_ALREADY_UNLOCKED = 19;
        public static final int FAILED_TO_INIT_HD_WALLET = 20;

        // kernel
        public static final int FAILED_TO_INIT_ED25519 = 31;
        public static final int FAILED_TO_LOAD_CONFIG = 32;
        public static final int FAILED_TO_LOAD_GENESIS = 33;
        public static final int FAILED_TO_LAUNCH_KERNEL = 34;
        public static final int INVALID_NETWORK_LABEL = 35;
        public static final int FAILED_TO_SETUP_TRACER = 36;

        // database
        public static final int FAILED_TO_OPEN_DB = 51;
        public static final int FAILED_TO_REPAIR_DB = 52;
        public static final int FAILED_TO_WRITE_BATCH_TO_DB = 53;

        // upgrade
        public static final int HARDWARE_UPGRADE_NEEDED = 71;
        public static final int CLIENT_UPGRADE_NEEDED = 72;
        public static final int JVM_32_NOT_SUPPORTED = 73;
    }

    public enum OsName {
        WINDOWS("Windows"),

        LINUX("Linux"),

        MACOS("macOS"),

        UNKNOWN("Unknown");

        private final String name;

        OsName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Returns the operating system name.
     *
     * @return
     */
    public static OsName getOsName() {
        String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);

        if (os.contains("win")) {
            return OsName.WINDOWS;
        } else if (os.contains("linux")) {
            return OsName.LINUX;
        } else if (os.contains("mac")) {
            return OsName.MACOS;
        } else {
            return OsName.UNKNOWN;
        }
    }

    /**
     * Returns the operating system architecture
     *
     * @return
     */
    public static String getOsArch() {
        return System.getProperty("os.arch");
    }

    /**
     * Terminates the JVM synchronously.
     */
    public static void exit(int code) {
        System.exit(code);
    }

    /**
     * Terminates the JVM asynchronously.
     */
    public static void exitAsync(int code) {
        new Thread(() -> System.exit(code)).start();
    }

    /**
     * Returns whether the JVM is in 32-bit data model
     *
     * @return
     */
    public static boolean is32bitJvm() {
        String model = System.getProperty("sun.arch.data.model");
        return model != null && model.contains("32");
    }

    /**
     * Returns the number of processors.
     *
     * @return
     */
    public static int getNumberOfProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }

    public static boolean bench() {
        // check JVM data model
        if (is32bitJvm()) {
            log.info("You're running 32-bit JVM. Please consider upgrading to 64-bit JVM");
            return false;
        }

        // check CPU
        if (getNumberOfProcessors() < 2) {
            log.info("# of CPU cores = {}", getNumberOfProcessors());
            return false;
        }

        // check memory
        if (getTotalMemorySize() < 3L * 1024L * 1024L * 1024L) {
            log.info("Total physical memory size = {} MB", getTotalMemorySize() / 1024 / 1024);
            return false;
        }

        return true;
    }

    public static long getTotalMemorySize() {
        SystemInfo systemInfo = new SystemInfo();
        return systemInfo.getHardware().getMemory().getTotal();
    }
}
