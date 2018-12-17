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
import com.cleo.connector.api.command.ConnectorCommandName;
import com.cleo.connector.api.command.ConnectorCommandResult;
import com.cleo.connector.api.command.ConnectorCommandUtil;
import com.cleo.connector.api.command.DirCommand;
import com.cleo.connector.api.command.GetCommand;
import com.cleo.connector.api.command.OtherCommand;
import com.cleo.connector.api.command.PutCommand;
import com.cleo.connector.api.property.ConnectorPropertyException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Strings;

public class BucketConnectorClient extends ConnectorClient {
    BucketConnectorConfig config;

    public BucketConnectorClient(BucketConnectorSchema schema) {
        this.config = new BucketConnectorConfig(this, schema);
    }

    private Path parsePath(String parse) throws ConnectorPropertyException {
        return new Path()
                .delimiter(ProjectClient.SLASH)
                .markDirectories(config.getMarkDirectories())
                .suffixDirectories(true)
                .parseURIPath(parse);
    }

    @Command(name = PUT, options = { Unique, Delete })
    public ConnectorCommandResult put(PutCommand put) throws ConnectorException, IOException {
        logger.debug(String.format("PUT local '%s' to remote '%s'", put.getSource().getPath(), put.getDestination().getPath()));

        ClientResolver.Resolved resolved = resolve(parsePath(put.getDestination().getPath()).directory(false), PUT);
        Client client = resolved.client();
        Path destination = resolved.path();

        if (ConnectorCommandUtil.isOptionOn(put.getOptions(), Unique) && client.exists(destination)) {
            String base = FilenameUtils.getBaseName(destination.name());
            String ext = FilenameUtils.getExtension(destination.name())
                                      .replaceFirst("^(?=[^\\.])",".");
                                      // if non-empty and doesn't start with ., prefix with .
            do {
                destination = destination.parent().child(base + "." + Long.toString(new Random().nextInt(Integer.MAX_VALUE)) + ext);
            } while (client.exists(destination));
            logger.debug(String.format("PUT calculated unique destination '%s'", destination));
        }


        // TODO this can't be canceled like calling transfer, but how to avoid spawning a pipe thread?
        client.upload(destination, put.getSource().getStream());
        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name = GET, options = { Directory, Delete, Unique })
    public ConnectorCommandResult get(GetCommand get) throws ConnectorException, IOException {
        logger.debug(String.format("GET remote '%s' to local '%s'", get.getSource().getPath(), get.getDestination().getPath()));

        ClientResolver.Resolved resolved = resolve(parsePath(get.getSource().getPath()).directory(false), GET);
        Path source = resolved.path();
        Client client = resolved.client();

        if (!client.exists(source)) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", source),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        }

        transfer(client.download(source), get.getDestination().getStream(), true); // TODO options?
        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name = DIR)
    public ConnectorCommandResult dir(DirCommand dir) throws ConnectorException, IOException {
        logger.debug(String.format("DIR '%s'", dir.getSource().getPath()));

        ClientResolver.Resolved resolved = resolve(parsePath(dir.getSource().getPath()).directory(true), DIR);
        Path path = resolved.path();
        Client client = resolved.client();

        if (!client.exists(path)) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        }

        List<Entry> entries = client.list(path);
        List<com.cleo.connector.api.directory.Entry> result = new ArrayList<>(entries.size());
        if (!entries.isEmpty()) {
            for (Entry entry : entries) {
                logger.debug(String.format("caching attributes for '%s' from DIR", entry.getPathObject().toString()));
                AttrCache.put(getHost().getAlias(), entry.getPathObject(), new EntryAttributes(entry));
                result.add(resolved.fixup(entry));
            }
        }
        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success, Optional.empty(), result);
    }

    @Command(name = MKDIR)
    public ConnectorCommandResult mkdir(OtherCommand mkdir) throws ConnectorException, IOException {
        logger.debug(String.format("MKDIR '%s'", mkdir.getSource()));

        ClientResolver.Resolved resolved = resolve(parsePath(mkdir.getSource()).directory(true), MKDIR);
        Path source = resolved.path();
        Client client = resolved.client();

        if (client.exists(source)) {
            return new ConnectorCommandResult(ConnectorCommandResult.Status.Error,
                    String.format("'%s' already exists.", source));
        } else {
            if (!client.mkdir(source)) {
                return new ConnectorCommandResult(ConnectorCommandResult.Status.Error,
                        String.format("'%s' not created exists.", source));
            }
        }
        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name = RMDIR)
    public ConnectorCommandResult rmdir(OtherCommand rmdir) throws ConnectorException, IOException {
        logger.debug(String.format("RMDIR '%s'", rmdir.getSource()));

        ClientResolver.Resolved resolved = resolve(parsePath(rmdir.getSource()).directory(true), RMDIR);
        Path source = resolved.path();
        Client client = resolved.client();

        if (!client.exists(source)) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", source),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        } else {
            if (!client.rmdir(source)) {
                return new ConnectorCommandResult(ConnectorCommandResult.Status.Error,
                        String.format("'%s' was not deleted", source));
            }
            AttrCache.invalidate(getHost().getAlias(), source);
        }

        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name = RENAME)
    public ConnectorCommandResult rename(OtherCommand rename) throws ConnectorException, IOException {
        logger.debug(String.format("RENAME '%s' '%s'", rename.getSource(), rename.getDestination()));

        ClientResolver.Resolved resolved = resolve(parsePath(rename.getSource()), RENAME);
        Path source = resolved.path();
        Client client = resolved.client();

        ClientResolver.Resolved destination = resolve(parsePath(rename.getDestination()), RENAME);
        if (destination.client().getClass() != resolved.client().getClass()) {
            return new ConnectorCommandResult(ConnectorCommandResult.Status.Error,
                    String.format("'%s' could not be renamed to '%s'", source, destination));
        } else if (!client.exists(source)) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", source),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        } else if (client.exists(destination.path())) {
            return new ConnectorCommandResult(ConnectorCommandResult.Status.Error,
                    String.format("'%s' already exists", destination));
        } else if (!client.rename(source, destination.path())) {
            return new ConnectorCommandResult(ConnectorCommandResult.Status.Error,
                    String.format("'%s' could not be renamed to '%s'", source, destination.path()));
        }
        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name = DELETE)
    public ConnectorCommandResult delete(OtherCommand delete) throws ConnectorException, IOException {
        logger.debug(String.format("DELETE '%s'", delete.getSource()));

        ClientResolver.Resolved resolved = resolve(parsePath(delete.getSource()), DELETE);
        Path source = resolved.path();
        Client client = resolved.client();

        if (!client.exists(source)) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", source),
                    ConnectorException.Category.fileNonExistentOrNoAccess);
        } else {
            if (!client.delete(source)) {
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
        logger.debug(String.format("ATTR '%s'", path));

        ClientResolver.Resolved resolved = resolve(parsePath(path), ATTR);
        Path source = resolved.path();
        Client client = resolved.client();

        Optional<BasicFileAttributeView> attr = Optional.empty();
        try {
            // if we are marking directories, the directory() flag can be trusted
            // if not, we first try attrs on a file named "source", then try again for a directory "source/"
            boolean directory = source.markDirectories() ? source.directory() : false;
            do {
                source.directory(directory);
                attr = AttrCache.get(getHost().getAlias(), source, new Callable<Optional<BasicFileAttributeView>>() {
                    @Override
                    public Optional<BasicFileAttributeView> call() throws ConnectorException {
                        Optional<BasicFileAttributeView> result = client.attr(source);
                        logger.debug(String.format("caching attributes for '%s' exists=%b", source.toString(), result.isPresent()));
                        return result;
                    }
                });
                logger.debug(String.format("retrieved attributes for '%s' exists=%b", source.toString(), attr.isPresent()));
                directory = !directory;
            } while (!source.markDirectories() && !attr.isPresent() && directory);
        } catch (Exception e) {
            throw new ConnectorException(String.format("error getting attributes for '%s'", source), e);
        }
        if (attr.isPresent()) {
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
    private Storage login() throws ConnectorException, IOException {
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
        return storage;
    }
    private ClientResolver.Resolved resolve(Path path, ConnectorCommandName command) throws ConnectorException, IOException {
        return new ClientResolver(config.getBucketName(), null, this::accountSupplier, this::containerSupplier).resolve(path, command);
    }

    private ProjectClient accountSupplier() throws ConnectorException, IOException {
        return new ProjectClient(login());
    }
    private BucketClient containerSupplier(Path bucket) throws ConnectorException, IOException {
        return new BucketClient(login(), bucket.node(0));
    }

}
