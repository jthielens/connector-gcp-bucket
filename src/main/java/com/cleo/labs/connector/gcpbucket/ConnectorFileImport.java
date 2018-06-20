package com.cleo.labs.connector.gcpbucket;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.cleo.connector.api.annotations.Display;
import com.cleo.connector.api.annotations.Setter;
import com.google.common.io.CharStreams;

public class ConnectorFileImport {
    private static final DateFormat DATEFORMAT = new SimpleDateFormat("'Imported on' yyyy/MM/dd HH:mm:ss");
    public static final String DELIMITER = "@@@";

    @Setter("Import")
    public String importFile(InputStream in) throws IOException {
        return new StringBuilder(DATEFORMAT.format(new Date()))
                .append(DELIMITER)
                .append(CharStreams.toString(new InputStreamReader(in)))
                .toString();
    }

    @Display
    public String display(String value) {
        if (value != null && value.contains(DELIMITER)) {
            value = value.substring(0, value.indexOf(DELIMITER));
        }
        return value;
    }

    public static String value(String value) {
        if (value != null && value.contains(DELIMITER)) {
            value = value.substring(value.indexOf(DELIMITER) + DELIMITER.length());
        }
        return value;
    }
}