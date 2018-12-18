package com.cleo.labs.connector.gcpbucket;

import static com.cleo.connector.api.command.ConnectorCommandName.DIR;

import java.io.IOException;

import com.cleo.connector.api.ConnectorException;
import com.cleo.connector.api.command.ConnectorCommandName;
import com.google.common.base.Strings;

public class ClientResolver {

    private String container;
    private Path prefix;
    private AccountSupplier accountSupplier;
    private ContainerSupplier containerSupplier;

    public interface AccountSupplier {
        Client get() throws ConnectorException, IOException;
    }
    public interface ContainerSupplier {
        Client get(Path container) throws ConnectorException, IOException;
    }

    public ClientResolver(String container, Path prefix, AccountSupplier accountSupplier, ContainerSupplier containerSupplier) {
        this.container = container;
        this.prefix = prefix;
        this.accountSupplier = accountSupplier;
        this.containerSupplier = containerSupplier;
    }

    /**
     * Resolves a supplied path adding path elements as configured for the resolver:
     * <pre>
     *     container/prefix/path
     * </pre>
     * If a container is not configured and path is empty, the prefix is ignored
     * (meaning a DIR command will enumerate available containers).
     * Otherwise the prefix is inserted between the container (either configured
     * in the resolver or taken from the first node of the path) and the
     * remaining path.<p/>
     * The type of container (account-level or container-level) returned depends both
     * on the length of the fully expanded path and the command.  In particular,
     * the DIR command operates one level higher (as if "/*" were appended), so
     * a single node DIR path is processed at the container level, while other
     * single node paths are processed at the account level.
     * the account, while all other operations 
     * @param path the path passed into the command
     * @param command which command
     * @return a {@link Resolved}
     * @throws IOException 
     * @throws ConnectorException 
     */
    public Resolved resolve(Path path, ConnectorCommandName command) throws ConnectorException, IOException {
        Path fullPath = path;
        String prepend = null; // container name to prepend to resulting Entrys
        int chroots = 0; // prefix nodes to "chroot" off resulting Entrys
        if (!Strings.isNullOrEmpty(container)) {
            fullPath = path.insert(0, new Path().child(container));
        }
        if (!fullPath.empty() && prefix != null && !prefix.empty()) {
            fullPath = fullPath.insert(1, prefix);
            chroots = prefix.size();
        }
        if (fullPath.empty() || (fullPath.size()==1 && command != DIR)) {
            return new Resolved(accountSupplier.get(), fullPath, fullPath)
                    .edit(true);
        } else {
            if (Strings.isNullOrEmpty(container)) {
                prepend = fullPath.node(0);
            }
            return new Resolved(containerSupplier.get(fullPath.slice(0, 1)), fullPath.slice(1, null), fullPath)
                    .edit(prepend, chroots);
        }
    }

    /**
     * Encapsulates the results of resolving a command path against the
     * configuration captured in the {@link ClientResolver}.
     */
    public class Resolved {
        private Client client;
        private Path path;
        private Path fullPath;
        /**
         * Construct a new Resolved object encapsulating the
         * correct Client (account or container level), the Path
         * that Client should operate on, and the fullPath
         * that should be used as a cache ID.
         * @param client the Client
         * @param path the Path
         * @param fullPath the full Path
         */
        public Resolved(Client client, Path path, Path fullPath) {
            this.client = client;
            this.path = path;
            this.fullPath = fullPath;
        }
        /**
         * {@link Client} getter
         * @return the Client
         */
        public Client client() {
            return client;
        }
        /**
         * {@link Path} getter
         * @return the Path
         */
        public Path path() {
            return path;
        }
        /**
         * fullPath getter
         * @return the fullPath
         */
        public Path fullPath() {
            return fullPath;
        }
        private String prepend = null;
        private int chroots = 0;
        /**
         * Modify the Resolved to indicate that DIR entries should be edited by
         * <ul><li>prepending a container name, or</li>
         *     <li>stripping off a prefix</li><ul>
         * @param prepend the container name to prepend (or {@code null} or empty)
         * @param chroots the size of the prefix in nodes (or 0 for none)
         * @return {@code this} for fluent use
         */
        public Resolved edit(String prepend, int chroots) {
            this.prepend = prepend;
            this.chroots = chroots;
            return this;
        }
        private boolean suppressDirectoryMark = false;
        /**
         * Modify the Resolved to indicate that DIR entries should be edited by
         * <ul><li>suppressing the .dir suffix (if enabled)</li><ul>
         * Since account level DIR results can only contain containers (and not
         * objects directly), there is no potential ambiguity and thus no need
         * to append ".dir" to distinguish directories from file/objects of the
         * same name.
         * @param suppressDirectoryMark
         * @return
         */
        public Resolved edit(boolean suppressDirectoryMark) {
            this.suppressDirectoryMark = suppressDirectoryMark;
            return this;
        }
        /**
         * Process {@link Entry} DIR results, applying any selected edits, and
         * then formatting their internal {@link Path} into String form
         * (as expected by {@link com.cleo.connector.api.directory.Entry#setPath(String)}).
         * @param entry the {@link Entry}
         * @return an updated {@link com.cleo.connector.api.directory.Entry}
         */
        public Entry fixup(Entry entry) {
            if (chroots > 0) {
                entry.setPathObject(entry.getPathObject().chroot(chroots));
            }
            if (!Strings.isNullOrEmpty(prepend)) {
                entry.setPathObject(entry.getPathObject().insert(0, new Path().child(prepend)));
            }
            if (suppressDirectoryMark) {
                entry.setPathObject(new Path(entry.getPathObject()).markDirectories(false));
            }
            entry.setPath(entry.getPathObject().toURIPath());
            return entry;
        }
    }
}
