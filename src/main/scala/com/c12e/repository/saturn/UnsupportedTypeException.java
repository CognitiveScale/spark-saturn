package com.c12e.repository.saturn;

/**
 * Created by dschueller on 10/27/15.
 */
public class UnsupportedTypeException extends Exception {
    private static final long serialVersionUID = 1L;
    public UnsupportedTypeException(String message) {
        super(message);
    }
    public UnsupportedTypeException(String message, Throwable cause) {
        super(message, cause);
    }
}
