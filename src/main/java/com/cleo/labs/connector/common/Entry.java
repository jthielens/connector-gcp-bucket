package com.cleo.labs.connector.common;

public class Entry extends com.cleo.connector.api.directory.Entry {
    private static final long serialVersionUID = -2204753598869098952L;

    public Entry(Type type) {
        super(type);
    }

    private Path pathObject;
    private String description;

    public Entry setPathObject(Path pathObject) {
        this.pathObject = pathObject;
        return this;
    }
    public Path getPathObject() {
        return pathObject;
    }
    public Entry setDescription(String description) {
        this.description = description;
        return this;
    }
    public String getDescription() {
        return description;
    }
}
