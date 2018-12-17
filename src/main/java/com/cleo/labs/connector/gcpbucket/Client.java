package com.cleo.labs.connector.gcpbucket;

import java.io.InputStream;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.List;
import java.util.Optional;

import com.cleo.connector.api.ConnectorException;

public class Client {

    public List<Entry> list(Path path) throws ConnectorException {
        throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path.toString()),
                ConnectorException.Category.fileNonExistentOrNoAccess);
    }

    public boolean delete(Path path) throws ConnectorException {
        throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path.toString()),
                ConnectorException.Category.fileNonExistentOrNoAccess);
    }

    public boolean rmdir(Path path) throws ConnectorException {
        throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path.toString()),
                ConnectorException.Category.fileNonExistentOrNoAccess);
    }

    public boolean mkdir(Path path) throws ConnectorException {
        throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path.toString()),
                ConnectorException.Category.fileNonExistentOrNoAccess);
    }

    public boolean exists(Path path) throws ConnectorException {
        throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path.toString()),
                ConnectorException.Category.fileNonExistentOrNoAccess);
    }

    public boolean rename(Path source, Path target) throws ConnectorException {
        throw new ConnectorException(String.format("'%s' does not exist or is not accessible", source.toString()),
                ConnectorException.Category.fileNonExistentOrNoAccess);
    }

    public boolean upload(Path path, InputStream in) throws ConnectorException {
        try {
            in.close();
        } catch (Exception ignore) {
            // oh well
        }
        throw new ConnectorException(String.format("'%s' is not accessible.", path.toString()));
    }

    public InputStream download(Path path) throws ConnectorException {
        throw new ConnectorException(String.format("'%s' is not accessible.", path.toString()));
    }

    public Optional<BasicFileAttributeView> attr(Path path) throws ConnectorException {
        throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path.toString()),
                ConnectorException.Category.fileNonExistentOrNoAccess);
    }
}