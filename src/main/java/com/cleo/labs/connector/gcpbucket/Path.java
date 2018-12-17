package com.cleo.labs.connector.gcpbucket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Strings;

public class Path {

    public static final String URI_DELIMITER = "/";

    /**
     * The list of String node names
     */
    private List<String> nodes;

    /**
     * Set to {@code true} to mark as a directory path
     */
    private boolean directory = false;

    /**
     * Sets the directory flag
     * @param directory the directory flag
     * @return {@code this} to enable fluent use
     */
    public Path directory(boolean directory) {
        this.directory = directory;
        return this;
    }

    /**
     * Returns the directory flag
     * @return the directory flag
     */
    public boolean directory() {
        return directory;
    }

    /**
     * Set to {@code true} to mark directory names, in case files and directories can share the same name
     */
    private boolean markDirectories = false;

    /**
     * Sets the markDirectories flag
     * @param markDirectories the markDirectories flag
     * @return {@code this} to enable fluent use
     */
    public Path markDirectories(boolean markDirectories) {
        this.markDirectories = markDirectories;
        return this;
    }

    /**
     * Returns the markDirectories flag
     * @return the markDirectories flag
     */
    public boolean markDirectories() {
        return markDirectories;
    }

    private String delimiter;
    public Path delimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }
    public String delimiter() {
        return delimiter;
    }

    private boolean suffixDirectories;
    public Path suffixDirectories(boolean suffixDirectories) {
        this.suffixDirectories = suffixDirectories;
        return this;
    }
    public boolean suffixDirectories() {
        return suffixDirectories;
    }
    /**
     * Internal use only: makes a new Path without attempting
     * to parse delimiters from the supplied node list.
     * @param nodes
     * @param markDirectories inherited markDirectories flag
     * @param directory is this path a directory?
     * @param delimiter the native path delimiter
     * @param suffixDirectories should directories include a delimiter suffix?
     */
    private Path(List<String> nodes, boolean markDirectories, boolean directory, String delimiter, boolean suffixDirectories) {
        this.nodes = nodes;
        this.markDirectories = markDirectories;
        this.directory = directory;
        this.delimiter = delimiter;
        this.suffixDirectories = suffixDirectories;
    }

    /**
     * Constructs a new empty path with the default delimiter.
     */
    public Path() {
        this.nodes = Collections.emptyList();
        this.markDirectories = false;
        this.directory = false;
        this.delimiter = URI_DELIMITER;
        this.suffixDirectories = false;
    }

    /**
     * Copy constructor
     * @param path the original path to copy
     */
    public Path(Path path) {
        this(new ArrayList<>(path.nodes), path.markDirectories, path.directory, path.delimiter, path.suffixDirectories);
    }

    /**
     * Parses zero or more strings to
     * form a path, splitting each string by the supplied delimiter.
     * All null or empty strings, in the {@code parse} list or
     * as a result of splitting, are discarded.  This means that
     * leading, trailing, or multiple consecutive delimiters are
     * ignored.  An empty path has length zero.
     * @param parse a (possibly {@code null}) list of (possibly {@code null})
     * {@code String}s to parse
     */
    public Path parseURIPath(String...parse) {
        String delimiter = URI_DELIMITER;
        boolean directoryLooking = true; // an empty path is the root directory
        this.nodes = new ArrayList<>();
        if (parse != null) {
            for (String node : parse) {
                if (!Strings.isNullOrEmpty(node)) {
                    for (String element : node.split(delimiter)) {
                        if (!element.isEmpty()) {
                            element = Escaper.unescape(element);
                            if (markDirectories) {
                                directoryLooking = DirMarker.marked(element);
                                nodes.add(DirMarker.unmark(element));
                            } else {
                                directoryLooking = element.endsWith(delimiter);
                                nodes.add(element);
                            }
                        }
                    }
                }
            }
            this.directory = directoryLooking; // from the last node seen
        }
        return this;
    }

    /**
     * Returns the number of nodes in the path, ranging
     * from {@code 0} for an empty path up.
     * @return
     */
    public int size() {
        return nodes.size();
    }

    /**
     * Returns {@code true} if the path is empty,
     * i.e. the path has length {@code 0}.
     * @return
     */
    public boolean empty() {
        return nodes.isEmpty();
    }

    /**
     * Returns the parent path, i.e. a path with the last
     * node of the path removed.  The parent of an empty path
     * is the same empty path.
     * @return a new Path shortened by 1, or {@code this} if the path is already empty
     */
    public Path parent() {
        if (nodes.isEmpty()) {
            return this;
        } else {
            return new Path(nodes.subList(0, nodes.size()-1), markDirectories, true, delimiter, suffixDirectories);
        }
    }

    /**
     * Returns a new path with one additional node added at the
     * end.  This method does not parse the supplied node for embedded
     * delimiters or check for null or empty nodes.
     * <p/>
     * To safely parse node names for addition to a path, use
     * <pre>
     *     child(new Path(nodes...))
     * </pre>
     * @param node the node name (should not be null, empty, or contain the delimiter, but these are not checked)
     * @return a longer path
     */
    public Path child(String node) {
        List<String> child = new ArrayList<>(nodes);
        child.add(node);
        return new Path(child, markDirectories, false, delimiter, suffixDirectories);
    }

    /**
     * Returns a new Path with zero or more additional nodes added at the
     * end, copied from the supplied Path.
     * @param path the Path to append to the current Path
     * @return the new Path
     */
    public Path child(Path path) {
        List<String> child = new ArrayList<>(nodes);
        child.addAll(path.nodes);
        return new Path(child, markDirectories, false, delimiter, suffixDirectories);
    }

    /**
     * Returns a new Path based on the current path but
     * shortened by {@code count} nodes from the left.  If {@code count}
     * is negative, nodes are retained from the right.  If there
     * are not that many nodes in the Path, an empty path is returned.
     * @param count the number of nodes to remove from the left or retain on the right
     * @return a shortened new Path
     */
    public Path chroot(int count) {
        return slice(count, null);
    }

    /**
     * Returns a slice of the current Path as a new Path based on a
     * starting from index (inclusive) and an ending to index (exclusive).
     * The from and to indices may be {@code null}, indicating that a reasonable
     * default (from 0 or to {@code size()}) should be used.  A negative
     * index means to back up from the end.  Invalid indices are adjusted to
     * the valid range.
     * @param fromNullable the from index (inclusive), or {@code null} to mean 0
     * @param toNullable the to index (exclusive), or {@code null} to mean {@code size()}
     * @return a (possibly empty) slice of the Path
     */
    public Path slice(Integer fromNullable, Integer toNullable) {
        int from = fromNullable==null ? 0 : fromNullable;
        int to = toNullable==null ? nodes.size() : toNullable;
        if (from < 0) {
            from = Math.max(0, nodes.size()+from);
        }
        from = Math.min(from, nodes.size());
        if (to < 0) {
            to = Math.max(0, nodes.size()+to);
        }
        to = Math.min(to, nodes.size());
        to = Math.max(from, to);
        return new Path(nodes.subList(from, to), markDirectories, to < nodes.size() || directory, delimiter, suffixDirectories);
    }

    /**
     * Returns the nth (0-relative) node in the Path,
     * or "" if there is no such node.  If n is negative,
     * counts from the end of the path.
     * @param n the node to return
     * @return the node
     */
    public String node(int n) {
        if (n < 0) {
            n = nodes.size()+n;
        }
        if (n < 0 || n >= nodes.size()) {
            return "";
        } else {
            return nodes.get(n);
        }
    }

    /**
     * Returns the "name" of the Path, i.e. the last node
     * or "" if the path is empty (has no nodes).
     * @return the "name" of the Path
     */
    public String name() {
        return node(-1);
    }

    /**
     * Returns all of the node names joined back together
     * with {@link #DEFAULT_DELIMITER} as a separator.  The returned
     * String does not begin with the delimiter, but ends with the
     * delimiter if-and-only-if this is a non-empty {@link #directory(boolean) directory}
     * Path. "" is returned for an empty Path, directory or not.
     */
    public String toURIPath() {
        if (empty()) {
            return "";
        } else {
            StringBuilder sb = new StringBuilder();
            for (int i=0; i < nodes.size(); i++) {
                String node = Escaper.escape(nodes.get(i), URI_DELIMITER);
                boolean last = i == nodes.size()-1;
                if (markDirectories && last) {
                    node = DirMarker.mark(node, directory || !last);
                }
                sb.append(node);
                if (!last) {
                    sb.append(URI_DELIMITER);
                }
            }
            return sb.toString();
        }
    }

    @Override
    public String toString() {
        if (empty()) {
            return "";
        } else {
            StringBuilder sb = new StringBuilder();
            for (String node : nodes) {
                sb.append(node)
                  .append(delimiter);
            }
            if (!directory || !suffixDirectories) {
                sb.setLength(sb.length()-1);
            }
            return sb.toString();
        }
    }

    public static class DirMarker {
        private static final String DIRMARK = ".dir";
        private static final String SEAL = ".";
        public static String mark(String node, boolean directory) {
            if (directory) {
                return node + DIRMARK;
            } else if (node.endsWith(DIRMARK) || node.endsWith(SEAL)) {
                return node + SEAL;
            }
            return node;
        }
        public static boolean marked(String node) {
            return node.endsWith(DIRMARK);
        }
        public static String unmark(String node) {
            if (node.endsWith(DIRMARK)) {
                return node.substring(0, node.length()-DIRMARK.length());
            } else if (node.endsWith(SEAL)) {
                return node.substring(0,  node.length()-SEAL.length());
            }
            return node;
        }
    }

    public static class Escaper {
        private static final String LPAREN = "(";
        private static final String QUOTE_LPAREN = "\\"+LPAREN;
        private static final String LPAREN_ENCODED = encode(LPAREN);
        private static final String RPAREN = ")";
        private static final String QUOTE_RPAREN = "\\"+RPAREN;
        private static final String COLON = ":";

        public static String decode(String string) {
            return Stream.of(string.substring(LPAREN.length(), string.length()-RPAREN.length()).split(COLON))
                    .filter(s -> !Strings.isNullOrEmpty(s))
                    .map(s -> new String(Character.toChars(Integer.valueOf(s, 16))))
                    .collect(Collectors.joining());
        }

        public static String encode(String s) {
            return LPAREN+s.codePoints().boxed().map(Integer::toHexString).collect(Collectors.joining(COLON))+RPAREN;
        }

        public static String escape(String string, String delimiter) {
            return string.replace(LPAREN, LPAREN_ENCODED).replaceAll(delimiter, encode(delimiter));
        }

        private static final Pattern ESCAPE = Pattern.compile(QUOTE_LPAREN+"[:0-9a-fA-F]*"+QUOTE_RPAREN);
        public static String unescape(String string) {
            Matcher m = ESCAPE.matcher(string);
            StringBuffer result = new StringBuffer();
            while (m.find()) {
                m.appendReplacement(result, Matcher.quoteReplacement(decode(m.group())));
            }
            m.appendTail(result);
            return result.toString();
        }
    }
}
