package com.imdea.sw;

import com.sun.jna.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

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
        int Client_Execute(long handle, Pointer txn, int length, Pointer ptr, int buffSize);
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

    // ---- one-shot transactions and column model (see state/row.go) ----

    /** An operation of a one-shot transaction, on a single key. */
    public static final class Op {
        // operation codes of the state machine (state/state.go)
        private static final byte PUT = 1, GET = 2, SCAN = 3, UPDATE = 4, INCR = 5, DELETE = 6;

        final byte code;
        final long key;
        final byte[] arg;

        private Op(byte code, long key, byte[] arg) {
            this.code = code;
            this.key = key;
            this.arg = arg == null ? new byte[0] : arg;
        }

        /** Writes an opaque value. */
        public static Op put(long key, byte[] value) {
            return new Op(PUT, key, value);
        }

        /** Reads a value (a row, see {@link #decodeRow}, if it was written with update or incr). */
        public static Op get(long key) {
            return new Op(GET, key, null);
        }

        /** Reads the values of the keys in [key, key+count], concatenated (see {@link #decodeRows}). */
        public static Op scan(long key, long count) {
            return new Op(SCAN, key, le(8).putLong(count).array());
        }

        /** Merges the given columns into the row stored at key. */
        public static Op update(long key, Map<String, byte[]> columns) {
            return new Op(UPDATE, key, encodeRow(columns));
        }

        /** Adds delta to a counter column (a decimal number) of the row stored at key; returns its new value. */
        public static Op incr(long key, String column, long delta) {
            byte[] c = column.getBytes(StandardCharsets.UTF_8);
            return new Op(INCR, key, le(8 + c.length).putLong(delta).put(c).array());
        }

        /** Removes key. */
        public static Op delete(long key) {
            return new Op(DELETE, key, null);
        }
    }

    /**
     * Runs a one-shot transaction and returns the result of each operation, in order. The Go client executes it
     * atomically if the protocol supports transactions (Janus), and its operations one after the other otherwise.
     */
    public byte[][] execute(List<Op> ops) {
        int size = 4;
        for (Op op : ops) {
            size += 1 + 8 + 4 + op.arg.length;
        }
        ByteBuffer txn = le(size).putInt(ops.size());
        for (Op op : ops) {
            txn.put(op.code).putLong(op.key).putInt(op.arg.length).put(op.arg);
        }
        Memory in = new Memory(size);
        in.write(0, txn.array(), 0, size);
        Pointer p = Native.getDirectBufferPointer(outBuf);
        int len = Lib.INSTANCE.Client_Execute(handle, in, size, p, buffSize);
        if (len == -2) {
            throw new IllegalStateException("buffer too small for the results of the transaction");
        }
        if (len < 0) {
            throw new RuntimeException("transaction failed (native error " + len + ")");
        }
        byte[] out = new byte[len];
        outBuf.position(0);
        outBuf.get(out, 0, len);
        ByteBuffer b = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
        byte[][] vals = new byte[b.getInt()][];
        for (int i = 0; i < vals.length; i++) {
            vals[i] = new byte[b.getInt()];
            b.get(vals[i]);
        }
        return vals;
    }

    public byte[][] execute(Op... ops) {
        return execute(Arrays.asList(ops));
    }

    /** Encodes a row (columns sorted by name). */
    public static byte[] encodeRow(Map<String, byte[]> columns) {
        TreeMap<byte[], byte[]> sorted = new TreeMap<>(Arrays::compareUnsigned);
        int size = 4;
        for (Map.Entry<String, byte[]> e : columns.entrySet()) {
            byte[] n = e.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] v = e.getValue() == null ? new byte[0] : e.getValue();
            sorted.put(n, v);
            size += 8 + n.length + v.length;
        }
        ByteBuffer b = le(size).putInt(sorted.size());
        for (Map.Entry<byte[], byte[]> e : sorted.entrySet()) {
            b.putInt(e.getKey().length).put(e.getKey()).putInt(e.getValue().length).put(e.getValue());
        }
        return b.array();
    }

    /** Decodes a row; an empty value is an empty row. */
    public static Map<String, byte[]> decodeRow(byte[] data) {
        List<Map<String, byte[]>> rows = decodeRows(data);
        if (rows.size() > 1) {
            throw new IllegalArgumentException("not a single row");
        }
        return rows.isEmpty() ? new TreeMap<>() : rows.get(0);
    }

    /** Decodes a sequence of rows, e.g., the result of a scan. */
    public static List<Map<String, byte[]>> decodeRows(byte[] data) {
        ByteBuffer b = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        List<Map<String, byte[]>> rows = new ArrayList<>();
        while (b.hasRemaining()) {
            Map<String, byte[]> row = new TreeMap<>();
            int n = b.getInt();
            for (int i = 0; i < n; i++) {
                byte[] name = new byte[b.getInt()];
                b.get(name);
                byte[] value = new byte[b.getInt()];
                b.get(value);
                row.put(new String(name, StandardCharsets.UTF_8), value);
            }
            rows.add(row);
        }
        return rows;
    }

    private static ByteBuffer le(int size) {
        return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    }

}
