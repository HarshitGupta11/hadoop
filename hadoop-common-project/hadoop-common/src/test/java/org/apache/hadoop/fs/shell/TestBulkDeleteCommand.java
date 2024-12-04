/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.shell;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.HadoopTestBase;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBulkDeleteCommand extends HadoopTestBase  {
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
        Assertions.assertThat(pathData.size()).
                describedAs("Only one root path must be present").isEqualTo(1);
        Assertions.assertThat(pathData.get(0).path.toUri().getPath()).
                describedAs("Base path of the command should match").isEqualTo(new URI(arg1).getPath());
        Assertions.assertThat(bulkDeleteCommand.childArgs.size()).
                describedAs("Only one other argument was passed to the command").
                isEqualTo(1);
        Assertions.assertThat(bulkDeleteCommand.childArgs.get(0)).
                describedAs("Children arguments must match").isEqualTo(arg2);
    }
}
