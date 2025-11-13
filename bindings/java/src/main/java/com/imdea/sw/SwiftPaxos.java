package com.imdea.sw;

import com.sun.jna.*;
import java.io.*;
import java.nio.file.Files;
import java.util.Locale;

/**
 * Single-file Java helper that:
 * - extracts the platform native library packaged under /native/<os>-<arch>/ into a temp file and System.load()s it,
 * - binds the Go C-shared exports using JNA,
 * - exposes convenient thin Java wrappers around the exported functions.
 *
 * Usage:
 *  - Place platform native libs in src/main/resources/native/<os>-<arch>/libswiftpaxos.so (or libswiftpaxos.dylib / swiftpaxos.dll)
 *  - Build the JAR (mvn package) to produce jar-with-dependencies under target/
 *  - In other projects include the produced jar; before calling methods, call SwiftPaxos.initNative() once.
 *  - Create clients with newClient(...), then call connect/send/read/disconnect via the returned long handle.
 */
public final class SwiftPaxos {
    private SwiftPaxos() {}

    private static final int maxLen = 64 * 1024; // 64 KiB buffer (adjust as needed for expected sizes)

    // JNA mapping of the C-exported symbols produced by the Go -buildmode=c-shared wrapper.
    private interface Lib extends Library {
        Lib INSTANCE = Native.load("swiftpaxos", Lib.class);

        long Client_New(String server, String maddr, int mport, int fast, int leaderless, int verbose);
        int Client_Connect(long handle);
        void Client_Disconnect(long handle);
        int Client_Reconnect(long handle);

        int Client_SendWrite(long handle, long key, Pointer data, Integer length);
        int Client_SendRead(long handle, long key, Pointer ptr);
        int Client_SendScan(long handle, long key, long count, Pointer ptr);
    }

    private static volatile boolean nativeLoaded = false;

    /**
     * Extracts native library for current platform from /native/<os>-<arch>/ and loads it into the process.
     * Call once before the first native invocation (or rely on lazyLoad()).
     *
     * The library file must be present inside the JAR under src/main/resources/native/<os>-<arch>/libswiftpaxos.{so|dylib|dll}
     */
    public static synchronized void initNative() {
        if (nativeLoaded) return;
        String os = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
        String arch = System.getProperty("os.arch").toLowerCase(Locale.ENGLISH);

        String osKey;
        if (os.contains("linux")) {
            osKey = "linux";
        } else if (os.contains("mac") || os.contains("darwin")) {
            osKey = "darwin";
        } else if (os.contains("win")) {
            osKey = "windows";
        } else {
            osKey = os.replaceAll("[^a-z0-9]+", "");
        }

        String archKey;
        if (arch.contains("amd64") || arch.contains("x86_64")) {
            archKey = "x86_64";
        } else if (arch.contains("aarch64") || arch.contains("arm64")) {
            archKey = "arm64";
        } else {
            archKey = arch.replaceAll("[^a-z0-9]+", "");
        }

        String libName;
        if (osKey.equals("windows")) {
            libName = "swiftpaxos.dll";
        } else if (osKey.equals("darwin")) {
            libName = "libswiftpaxos.dylib";
        } else {
            libName = "libswiftpaxos.so";
        }

        String resource = "/native/" + osKey + "-" + archKey + "/" + libName;
        try (InputStream in = SwiftPaxos.class.getResourceAsStream(resource)) {
            if (in == null) {
                // If the user packaged the jar without native libs, we still let JNA attempt loading by name.
                // Throw a clear error so caller can fix packaging.
                throw new UnsatisfiedLinkError("Native library not found in JAR at: " + resource);
            }
            File tmp = Files.createTempFile("swiftpaxos-", "-" + libName).toFile();
            tmp.deleteOnExit();
            try (OutputStream out = new FileOutputStream(tmp)) {
                byte[] buf = new byte[8192];
                int r;
                while ((r = in.read(buf)) != -1) {
                    out.write(buf, 0, r);
                }
            }
            tmp.setExecutable(true);
            System.load(tmp.getAbsolutePath());
            // mark loaded. JNA will still need to resolve by library name; System.load ensures the binary is in process.
            nativeLoaded = true;
        } catch (IOException e) {
            throw new UnsatisfiedLinkError("Failed to extract native lib: " + e.getMessage());
        }
    }

    private static void ensureLoaded() {
        if (!nativeLoaded) {
            initNative();
        }
    }

    private static int fromBoolean(boolean b) {
        return b ? 1 : 0;
    }

    // High-level Java wrappers ------------------------------------------------

    public static long newClient(String masterAddr, int masterPort, boolean fast, boolean leaderless, boolean verbose) {
        ensureLoaded();
        return Lib.INSTANCE.Client_New("_UNDEFINED_", masterAddr, masterPort, fromBoolean(fast), fromBoolean(leaderless), fromBoolean(verbose));
    }

    public static int connect(long handle) {
        ensureLoaded();
        return Lib.INSTANCE.Client_Connect(handle);
    }

    public static void disconnect(long handle) {
        ensureLoaded();
        Lib.INSTANCE.Client_Disconnect(handle);
    }

    public static int reconnect(long handle) {
        ensureLoaded();
        return Lib.INSTANCE.Client_Reconnect(handle);
    }

    public static void write(long handle, long key, byte[] data) {
        ensureLoaded();
        Pointer p = null;
        if (data != null && data.length > 0) {
            p = new Memory(data.length);
            p.write(0, data, 0, data.length);
        }
        Lib.INSTANCE.Client_SendWrite(handle, key, p, data == null ? 0 : data.length);
        Memory.disposeAll();
    }

    public static byte[] read(long handle, long key) {
        ensureLoaded();
        Memory outBuf = new Memory(maxLen); // FIXME
        int len = Lib.INSTANCE.Client_SendRead(handle, key, outBuf);
        return outBuf.getByteArray(0, len);
    }

    public static byte[] scan(long handle, long key, long count) {
        ensureLoaded();
        Memory outBuf = new Memory(maxLen); // FIXME
        int len = Lib.INSTANCE.Client_SendScan(handle, key, count, outBuf);
        return outBuf.getByteArray(0, len);
    }

}
