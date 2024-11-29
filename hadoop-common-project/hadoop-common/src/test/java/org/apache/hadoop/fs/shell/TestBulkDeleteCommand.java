package org.apache.hadoop.fs.shell;

import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBulkDeleteCommand {
    private static Configuration conf;

    @BeforeClass
    public static void setup() throws IOException {
        conf = new Configuration();
    }

    @Test
    public void testDefaults() throws IOException {
        LinkedList<String> options = new LinkedList<>();
        BulkDeleteCommand bulkDeleteCommand = new BulkDeleteCommand();
        bulkDeleteCommand.processOptions(options);
        assertTrue(bulkDeleteCommand.childArgs.isEmpty());
    }

    @Test
    public void testArguments() throws IOException, URISyntaxException {
        BulkDeleteCommand bulkDeleteCommand = new BulkDeleteCommand(conf);
        LinkedList<String> arguments = new LinkedList<>();
        String arg1 = "file:///file/name/1";
        String arg2 = "file:///file/name/2";
        arguments.add(arg1);
        arguments.add(arg2);
        LinkedList<PathData> pathData = bulkDeleteCommand.expandArguments(arguments);
        assertEquals(1, pathData.size());
        assertEquals(new URI(arg1).getPath(), pathData.get(0).path.toUri().getPath());
        assertEquals(1, bulkDeleteCommand.childArgs.size());
        assertEquals(arg2, bulkDeleteCommand.childArgs.get(0));
    }
}
