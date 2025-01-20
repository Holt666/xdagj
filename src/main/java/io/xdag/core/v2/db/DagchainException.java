package io.xdag.core.v2.db;

public class DagchainException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public DagchainException() {
    }

    public DagchainException(String s) {
        super(s);
    }

    public DagchainException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public DagchainException(Throwable throwable) {
        super(throwable);
    }

    public DagchainException(String s, Throwable throwable, boolean b, boolean b1) {
        super(s, throwable, b, b1);
    }
}
