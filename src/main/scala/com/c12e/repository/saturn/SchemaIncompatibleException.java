package com.c12e.repository.saturn;

/**
 * Created by dschueller on 10/27/15.
 */
public class SchemaIncompatibleException extends Exception {

    private static final long serialVersionUID = 1L;

    public SchemaIncompatibleException(String message) {
        super(message);
    }

    public SchemaIncompatibleException(String message, Throwable cause) {
        super(message, cause);
    }
}
