package com.cleo.labs.connector.gcpbucket;

import static com.cleo.connector.api.command.ConnectorCommandName.ATTR;
import static com.cleo.connector.api.command.ConnectorCommandName.DELETE;
import static com.cleo.connector.api.command.ConnectorCommandName.DIR;
import static com.cleo.connector.api.command.ConnectorCommandName.GET;
import static com.cleo.connector.api.command.ConnectorCommandName.MKDIR;
import static com.cleo.connector.api.command.ConnectorCommandName.PUT;
import static com.cleo.connector.api.command.ConnectorCommandName.RENAME;
import static com.cleo.connector.api.command.ConnectorCommandName.RMDIR;
import static com.cleo.connector.api.command.ConnectorCommandOption.Delete;
import static com.cleo.connector.api.command.ConnectorCommandOption.Directory;
import static com.cleo.connector.api.command.ConnectorCommandOption.Unique;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;

import com.cleo.connector.api.ConnectorClient;
import com.cleo.connector.api.ConnectorException;
import com.cleo.connector.api.annotations.Command;
import com.cleo.connector.api.command.ConnectorCommandResult;
import com.cleo.connector.api.command.ConnectorCommandUtil;
import com.cleo.connector.api.command.DirCommand;
import com.cleo.connector.api.command.GetCommand;
import com.cleo.connector.api.command.OtherCommand;
import com.cleo.connector.api.command.PutCommand;
import com.cleo.connector.api.directory.Directory.Type;
import com.cleo.connector.api.directory.Entry;
import com.cleo.connector.api.helper.Attributes;
import com.cleo.connector.api.interfaces.IConnectorIncoming;
import com.cleo.connector.api.interfaces.IConnectorOutgoing;
import com.cleo.connector.api.property.ConnectorPropertyException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Strings;

public class BucketConnectorClient extends ConnectorClient {
    BucketConnectorConfig config;

    public BucketConnectorClient(BucketConnectorSchema schema) {
        this.config = new BucketConnectorConfig(this, schema);
    }

    @Command(name = PUT, options = { Unique, Delete })
    public ConnectorCommandResult put(PutCommand put) throws ConnectorException, IOException {
        IConnectorOutgoing source = put.getSource();
        String destination = put.getDestination().getPath();
        BucketClient bucket = login();

        if (ConnectorCommandUtil.isOptionOn(put.getOptions(), Unique) && bucket.exists(destination, false)) {
            String fullPath = FilenameUtils.getFullPath(destination);
            String base = FilenameUtils.getBaseName(destination);
            String ext = FilenameUtils.getExtension(destination)
                                      .replaceFirst("^(?=[^\\.])",".");
                                      // if non-empty and doesn't start with ., prefix with .
            do {
                destination = fullPath + base + "." + Long.toString(new Random().nextInt(Integer.MAX_VALUE)) + ext;
            } while (bucket.exists(destination, false));
        }

        logger.debug(String.format("PUT local '%s' to remote '%s'", source.getPath(), destination));

        // TODO this can't be canceled like calling transfer, but how to avoid spawning a pipe thread?
        bucket.upload(destination, source.getStream());
        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name = GET, options = { Directory, Delete, Unique })
    public ConnectorCommandResult get(GetCommand get) throws ConnectorException, IOException {
        String source = get.getSource().getPath();
        IConnectorIncoming destination = get.getDestination();

        BucketClient bucket = login();

        logger.debug(String.format("GET remote '%s' to local '%s'", source, destination.getPath()));

        if (!bucket.exists(source, false)) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", source),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        }

        transfer(bucket.download(source), destination.getStream(), true); // TODO options?
        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name = DIR)
    public ConnectorCommandResult dir(DirCommand dir) throws ConnectorException, IOException {
        String path = dir.getSource().getPath();
        BucketClient bucket = login();
        logger.debug(String.format("DIR '%s'", path));

        if (!bucket.exists(path, true))
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                    ConnectorException.Category.fileNonExistentOrNoAccess);

        List<Blob> blobs = bucket.list(path);
        List<Entry> result = new ArrayList<>(blobs.size());
        if (!blobs.isEmpty()) {
            for (Blob blob : blobs) {
                Entry entry = new Entry(blob.isDirectory() ? Type.dir : Type.file);
                entry.setPath(blob.getName());
                if (blob.getUpdateTime() != null) {
                    entry.setDate(Attributes.toLocalDateTime(blob.getUpdateTime()));
                }
                if (entry.isFile()) {
                    entry.setSize(blob.getSize());
                } else {
                    entry.setSize(-1L);
                }
                result.add(entry);
            }
        }
        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success, Optional.empty(), result);
    }

    @Command(name = MKDIR)
    public ConnectorCommandResult mkdir(OtherCommand mkdir) throws ConnectorException, IOException {
        String source = mkdir.getSource();
        BucketClient bucket = login();
        logger.debug(String.format("MKDIR '%s'", source));

        if (bucket.exists(source, true)) {
            return new ConnectorCommandResult(ConnectorCommandResult.Status.Error,
                    String.format("'%s' already exists.", source));
        } else {
            if (bucket.mkdir(source) == null) {
                return new ConnectorCommandResult(ConnectorCommandResult.Status.Error,
                        String.format("'%s' not created exists.", source));
            }
        }
        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name = RMDIR)
    public ConnectorCommandResult rmdir(OtherCommand rmdir) throws ConnectorException, IOException {
        String source = rmdir.getSource();
        BucketClient bucket = login();
        logger.debug(String.format("RMDIR '%s'", source));

        if (!bucket.exists(source, true)) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", source),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        } else {
            if (!bucket.rmdir(source)) {
                return new ConnectorCommandResult(ConnectorCommandResult.Status.Error,
                        String.format("'%s' was not deleted", source));
            }
        }

        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name = RENAME)
    public ConnectorCommandResult rename(OtherCommand rename) throws ConnectorException, IOException {
        String source = rename.getSource();
        String destination = rename.getDestination();
        BucketClient bucket = login();
        logger.debug(String.format("RENAME '%s' '%s'", source, destination));

        if (!bucket.exists(source, false)) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", source),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        } else if (bucket.exists(destination, false)) {
            return new ConnectorCommandResult(ConnectorCommandResult.Status.Error,
                    String.format("'%s' already exists", destination));
        } else if (!bucket.rename(source, destination)) {
            return new ConnectorCommandResult(ConnectorCommandResult.Status.Error,
                    String.format("'%s' could not be renamed to '%s'", source, destination));
        }
        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name = DELETE)
    public ConnectorCommandResult delete(OtherCommand delete) throws ConnectorException, IOException {
        String source = delete.getSource();
        BucketClient bucket = login();
        logger.debug(String.format("DELETE '%s'", source));

        if (!bucket.exists(source, false)) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", source),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        } else {
            if (!bucket.delete(source)) {
                return new ConnectorCommandResult(ConnectorCommandResult.Status.Error,
                        String.format("'%s' was not deleted", source));
            }
        }

        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    /**
     * Get the file attribute view associated with a file path
     * 
     * @param path the file path
     * @return the file attributes
     * @throws com.cleo.connector.api.ConnectorException
     * @throws java.io.IOException
     */
    @Command(name = ATTR)
    public BasicFileAttributeView getAttributes(String path) throws ConnectorException, IOException {
        BucketClient bucket = login();
        if (!bucket.exists(path, false))
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        return new BucketFileAttributes(bucket.get(path, false));
    }

    private GoogleCredentials credentials() throws ConnectorPropertyException, IOException {
        String json = config.getServiceAccountKey();
        if (!Strings.isNullOrEmpty(json)) {
            return GoogleCredentials.fromStream(new ByteArrayInputStream(json.getBytes()));
        }
        return null;
    }
    private BucketClient login() throws ConnectorException, IOException {
        String projectId = config.getProjectId();
        GoogleCredentials credentials = credentials();
        Storage storage;
        if (projectId == null || credentials == null) {
            storage = StorageOptions
                    .getDefaultInstance()
                    .getService();
        } else {
            storage = StorageOptions
                    .newBuilder()
                    .setProjectId(projectId)
                    .setCredentials(credentials)
                    .build()
                    .getService();
        }
        return new BucketClient(storage, config.getBucketName());
    }

}
