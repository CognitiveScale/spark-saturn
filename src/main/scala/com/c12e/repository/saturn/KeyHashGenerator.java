package com.c12e.repository.saturn;

import org.apache.avro.Schema;
import org.apache.commons.codec.binary.Hex;
import org.codehaus.jackson.JsonNode;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by dschueller on 1/12/16.
 */
public class KeyHashGenerator {

    private MessageDigest mdKeys;
    private MessageDigest mdAll;
    private boolean hasKeys = false;

    public KeyHashGenerator() throws NoSuchAlgorithmException {
        mdKeys = MessageDigest.getInstance("SHA");
        mdKeys.reset();
        mdAll = MessageDigest.getInstance("SHA");
        mdAll.reset();
    }

    public void reset() {
        hasKeys = false;
        mdKeys.reset();
        mdAll.reset();
    }

    public void updateHash(Schema.Field f, Object value) {
        JsonNode isKeyField = f.getJsonProp("iskey");

        if ((isKeyField != null) && isKeyField.asBoolean()) {
            hasKeys = true;
            if (value != null)
                mdKeys.update(value.toString().getBytes());
        } else {
            if (value != null)
                mdAll.update(value.toString().getBytes());
        }
    }

    public String getHash() {
        if (hasKeys)
            return new String(Hex.encodeHex(mdKeys.digest()));
        else
            return new String(Hex.encodeHex(mdAll.digest()));
    }
}
