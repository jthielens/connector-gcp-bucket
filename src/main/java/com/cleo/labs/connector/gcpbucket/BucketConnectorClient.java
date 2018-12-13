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
import java.util.concurrent.Callable;

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
        Path destination = new Path().parse(put.getDestination().getPath());
        BucketClient bucket = login();

        if (ConnectorCommandUtil.isOptionOn(put.getOptions(), Unique) && bucket.exists(destination)) {
            String base = FilenameUtils.getBaseName(destination.name());
            String ext = FilenameUtils.getExtension(destination.name())
                                      .replaceFirst("^(?=[^\\.])",".");
                                      // if non-empty and doesn't start with ., prefix with .
            do {
                destination = destination.parent().child(base + "." + Long.toString(new Random().nextInt(Integer.MAX_VALUE)) + ext);
            } while (bucket.exists(destination));
        }

        logger.debug(String.format("PUT local '%s' to remote '%s'", source.getPath(), destination));

        // TODO this can't be canceled like calling transfer, but how to avoid spawning a pipe thread?
        bucket.upload(destination, source.getStream());
        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name = GET, options = { Directory, Delete, Unique })
    public ConnectorCommandResult get(GetCommand get) throws ConnectorException, IOException {
        Path source = new Path().parse(get.getSource().getPath());
        IConnectorIncoming destination = get.getDestination();

        BucketClient bucket = login();

        logger.debug(String.format("GET remote '%s' to local '%s'", source, destination.getPath()));

        if (!bucket.exists(source)) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", source),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        }

        transfer(bucket.download(source), destination.getStream(), true); // TODO options?
        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name = DIR)
    public ConnectorCommandResult dir(DirCommand dir) throws ConnectorException, IOException {
        Path path = new Path().parse(dir.getSource().getPath()).directory(true);
        BucketClient bucket = login();
        logger.debug(String.format("DIR '%s'", path));

        if (!bucket.exists(path))
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
                Path fullPath = new Path().parse(entry.getPath()).directory(entry.isDir());
                logger.debug(String.format("caching attributes for '%s'", fullPath.toString()));
                AttrCache.put(getHost().getAlias(), fullPath, new BucketFileAttributes(blob, entry.isDir()));
            }
        }
        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success, Optional.empty(), result);
    }

    @Command(name = MKDIR)
    public ConnectorCommandResult mkdir(OtherCommand mkdir) throws ConnectorException, IOException {
        Path source = new Path().parse(mkdir.getSource()).directory(true);
        BucketClient bucket = login();
        logger.debug(String.format("MKDIR '%s'", source));

        if (bucket.exists(source)) {
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
        Path source = new Path().parse(rmdir.getSource()).directory(true);
        BucketClient bucket = login();
        logger.debug(String.format("RMDIR '%s'", source));

        if (!bucket.exists(source)) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", source),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        } else {
            if (!bucket.rmdir(source)) {
                return new ConnectorCommandResult(ConnectorCommandResult.Status.Error,
                        String.format("'%s' was not deleted", source));
            }
            AttrCache.invalidate(getHost().getAlias(), source);
        }

        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name = RENAME)
    public ConnectorCommandResult rename(OtherCommand rename) throws ConnectorException, IOException {
        Path source = new Path().parse(rename.getSource());
        Path destination = new Path().parse(rename.getDestination());
        BucketClient bucket = login();
        logger.debug(String.format("RENAME '%s' '%s'", source, destination));

        if (!bucket.exists(source)) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", source),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        } else if (bucket.exists(destination)) {
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
        Path source = new Path().parse(delete.getSource());
        BucketClient bucket = login();
        logger.debug(String.format("DELETE '%s'", source));

        if (!bucket.exists(source)) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", source),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        } else {
            if (!bucket.delete(source)) {
                return new ConnectorCommandResult(ConnectorCommandResult.Status.Error,
                        String.format("'%s' was not deleted", source));
            }
            AttrCache.invalidate(getHost().getAlias(), source);
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
        Path source = new Path().parse(path);
        BucketClient bucket = login();
        logger.debug(String.format("ATTR '%s'", path));
        Optional<BasicFileAttributeView> attr = Optional.empty();
        try {
            boolean directory = false;
            do {
                source.directory(directory);
                attr = AttrCache.get(getHost().getAlias(), source, new Callable<Optional<BasicFileAttributeView>>() {
                    @Override
                    public Optional<BasicFileAttributeView> call() {
                        if (source.empty()) {
                            // return an Attr object representing the container
                            logger.debug(String.format("ATTR '%s' = root (added to cache)", path));
                            return Optional.of(new BucketRootAttributes(bucket.bucket()));
                        } else {
                            logger.debug(String.format("fetching attributes for '%s'", source.toString()));
                            if (bucket.exists(source)) {
                                logger.debug(String.format("caching attributes for '%s'", source.toString()));
                                return Optional.of(new BucketFileAttributes(bucket.get(source), source.directory()));
                            }
                        }
                        return Optional.empty();  // not found
                    }
                });
                directory = !directory;
            } while (!attr.isPresent() && directory);
        } catch (Exception e) {
            throw new ConnectorException(String.format("error getting attributes for '%s'", source), e);
        }
        if (attr.isPresent()) {
            logger.debug(String.format("retrieved attributes for '%s'", source.toString()));
            return attr.get();
        } else {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", source),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        }
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
