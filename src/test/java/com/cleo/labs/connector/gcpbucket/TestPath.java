package com.cleo.labs.connector.gcpbucket;

import static org.junit.Assert.*;

import org.junit.Test;

import com.cleo.labs.connector.gcpbucket.Path.DirMarker;
import com.cleo.labs.connector.gcpbucket.Path.Escaper;

public class TestPath {

    @Test
    public void testEncode() {
        assertEquals("(2f)", Escaper.encode("/"));
        assertEquals("/", Escaper.decode("(2f)"));
        assertEquals("(3a:3a)", Escaper.encode("::"));
        assertEquals("::", Escaper.decode("(3a:3a)"));
        assertEquals("()", Escaper.encode(""));
        assertEquals("/", Escaper.decode("(2f)"));
        assertEquals("", Escaper.decode("()"));
    }

    @Test
    public void testEscape() {
        assertEquals("abc", Escaper.unescape("a(62)c"));
        assertEquals("abc", Escaper.unescape("()a(:62:)c"));
        assertEquals("a(62)c", Escaper.escape("abc", "b"));
    }

    @Test
    public void testDirMarker() {
        String node;

        node = "foo";
        assertEquals(node, DirMarker.unmark(DirMarker.mark(node, true)));
        assertEquals(node, DirMarker.unmark(DirMarker.mark(node, false)));
        assertTrue(DirMarker.marked(DirMarker.mark(node, true)));
        assertFalse(DirMarker.marked(DirMarker.mark(node, false)));
        assertEquals(node+".dir", DirMarker.mark(node, true));
        assertEquals(node, DirMarker.mark(node, false));

        node = "foo.dir";
        assertEquals(node, DirMarker.unmark(DirMarker.mark(node, true)));
        assertEquals(node, DirMarker.unmark(DirMarker.mark(node, false)));
        assertTrue(DirMarker.marked(DirMarker.mark(node, true)));
        assertFalse(DirMarker.marked(DirMarker.mark(node, false)));
        assertEquals(node+".dir", DirMarker.mark(node, true));
        assertEquals(node+".", DirMarker.mark(node, false));

        node = "foo.";
        assertEquals(node, DirMarker.unmark(DirMarker.mark(node, true)));
        assertEquals(node, DirMarker.unmark(DirMarker.mark(node, false)));
        assertTrue(DirMarker.marked(DirMarker.mark(node, true)));
        assertFalse(DirMarker.marked(DirMarker.mark(node, false)));
        assertEquals(node+".dir", DirMarker.mark(node, true));
        assertEquals(node+".", DirMarker.mark(node, false));
    }

    @Test
    public void testURIPath() {
        assertEquals("a/b/c", new Path().markDirectories(true).child("a").child("b").child("c").toURIPath());
        assertEquals("a/b/c.dir", new Path().markDirectories(true).child("a").child("b").child("c").directory(true).toURIPath());
        assertEquals("a/b.dir", new Path().markDirectories(true).child("a").child("b").child("c").directory(false).parent().toURIPath());
        assertEquals("a/b/c", new Path().child("a").child("b").child("c").toURIPath());
        assertEquals("a/b/c", new Path().child("a").child("b").child("c").directory(true).toURIPath());
        assertEquals("a(2f)b/c", new Path().child("a/b").child("c").toURIPath());
    }

    @Test
    public void testStrings() {
        assertEquals("a/b/c", new Path().parseURIPath("a(2f)b/c").toString());
        assertEquals("a/b/c", new Path().suffixDirectories(true).parseURIPath("a(2f)b/c").toString());
        assertEquals("a/b/c", new Path().suffixDirectories(true).parseURIPath("a(2f)b/c/").toString());
        assertEquals("a/b:c", new Path().suffixDirectories(true).delimiter(":").parseURIPath("a(2f)b/c").toString());
        assertEquals("a/b:c", new Path().suffixDirectories(true).delimiter(":").parseURIPath("a(2f)b/c/").toString());
        assertEquals("a/b/c/", new Path().suffixDirectories(true).markDirectories(true).parseURIPath("a(2f)b/c.dir").toString());
        assertEquals("a/b/c/", new Path().suffixDirectories(true).markDirectories(true).parseURIPath("a.dir/b.dir/c.dir").toString());
    }
}
