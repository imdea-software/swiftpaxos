package com.imdea.sw;

import com.sun.jna.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Locale;

public final class SwiftPaxos {

    // JNA mapping of the C-exported symbols produced by the Go -buildmode=c-shared wrapper.
    private interface Lib extends Library {
        Lib INSTANCE = Native.load("swiftpaxos", Lib.class);

        long Client_New(String server, String maddr, int mport, int fast, int leaderless, int verbose);
        void Client_Connect(long handle);
        void Client_Disconnect(long handle);
        void Client_Reconnect(long handle);

        int Client_SendWrite(long handle, long key, Pointer data, Integer length);
        int Client_SendRead(long handle, long key, Pointer ptr, int buffSize);
        int Client_SendScan(long handle, long key, long count, Pointer ptr, int buffSize);
    }

    private static volatile boolean nativeLoaded = false;

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
        if (arch.contains("amd64") || arch.contains("x86-64")) {
            archKey = "x86-64";
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

        String resource = "/" + osKey + "-" + archKey + "/" + libName;
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

    //

    private final long handle;
    private final int buffSize;
    private final ByteBuffer outBuf; // direct buffer

    public SwiftPaxos(int buffSize, String masterAddr, int masterPort, boolean fast, boolean leaderless, boolean verbose) {
        if (buffSize <= 0) {
            throw new IllegalArgumentException();
        }
        ensureLoaded();
        this.handle = Lib.INSTANCE.Client_New("_UNDEFINED_", masterAddr, masterPort, fromBoolean(fast), fromBoolean(leaderless), fromBoolean(verbose));
        this.buffSize = buffSize;
        this.outBuf = ByteBuffer.allocateDirect(buffSize);
    }

    public void connect() {
        Lib.INSTANCE.Client_Connect(handle);
    }

    public void disconnect() {
        Lib.INSTANCE.Client_Disconnect(handle);
    }

    public void reconnect() {
        Lib.INSTANCE.Client_Reconnect(handle);
    }

    public void write(long key, byte[] data) {
        Pointer p = null;
        if (data != null && data.length > 0) {
            p = new Memory(data.length);
            p.write(0, data, 0, data.length);
        }
        Lib.INSTANCE.Client_SendWrite(handle, key, p, data == null ? 0 : data.length);
        // Memory.disposeAll(); FIXME
    }

    public byte[] read(long key) {
        Pointer p = Native.getDirectBufferPointer(outBuf);
        if (p == null || Pointer.nativeValue(p) == 0) {
            throw new IllegalStateException("direct buffer has no native pointer");
        }
        int len = Lib.INSTANCE.Client_SendRead(handle, key, p, buffSize);
        if (len < 0) throw new RuntimeException("native error " + len);
        byte[] out = new byte[len];
        outBuf.position(0);
        outBuf.get(out, 0, len);
        return out;
    }

    public byte[] scan(long key, long count) {
        Pointer p = Native.getDirectBufferPointer(outBuf);
        if (p == null || Pointer.nativeValue(p) == 0) {
            throw new IllegalStateException("direct buffer has no native pointer");
        }
        int len = Lib.INSTANCE.Client_SendScan(handle, key, count, p, buffSize);
        if (len < 0) throw new RuntimeException("native error " + len);
        byte[] out = new byte[len];
        outBuf.position(0);
        outBuf.get(out, 0, len);
        return out;
    }

}
