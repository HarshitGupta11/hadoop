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

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.HadoopTestBase;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBulkDeleteCommand extends HadoopTestBase {
  private static Configuration conf;
  private static FsShell shell;
  private static LocalFileSystem lfs;
  private static Path testRootDir;

  @BeforeClass
  public static void setup() throws IOException {
    conf = new Configuration();
    shell = new FsShell(conf);
    lfs = FileSystem.getLocal(conf);
    testRootDir = lfs.makeQualified(new Path(GenericTestUtils.getTempPath(
            "testFsShellBulkDelete")));
    lfs.delete(testRootDir, true);
    lfs.mkdirs(testRootDir);
    lfs.setWorkingDirectory(testRootDir);
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

  @Test
  public void testWrongArguments() throws IOException, URISyntaxException {
    BulkDeleteCommand bulkDeleteCommand = new BulkDeleteCommand(conf);
    LinkedList<String> arguments = new LinkedList<>();
    String arg1 = "file:///file/name/1";
    arguments.add(arg1);
    Assertions.assertThatThrownBy(() -> bulkDeleteCommand.expandArguments(arguments)).
            describedAs("No children to be deleted specified in the command.").
            isInstanceOf(IOException.class);
  }

  @Test
  public void testLocalFileDeletion() throws IOException {
    String deletionDir = "toDelete";
    String baseFileName = "file_";
    Path baseDir = new Path(testRootDir, deletionDir);
    List<String> listOfPaths = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Path p = new Path(baseDir, baseFileName + i);
      lfs.create(p);
    }
    RemoteIterator<LocatedFileStatus> remoteIterator = lfs.listFiles(baseDir, false);
    while (remoteIterator.hasNext()) {
      listOfPaths.add(remoteIterator.next().getPath().toUri().toString());
    }
    List<String> finalCommandList = new ArrayList<>();
    finalCommandList.add("-bulkDelete");
    finalCommandList.add(baseDir.toUri().toString());
    finalCommandList.addAll(listOfPaths);
    shell.run(finalCommandList.toArray(new String[0]));
    Assertions.assertThat(lfs.listFiles(baseDir, false).hasNext())
            .as("All the files should have been deleted under the path:" +
                    baseDir).isEqualTo(false);

  }

  @Test
  public void testLocalFileDeletionWithFileName() throws IOException {
    String deletionDir = "toDelete";
    String baseFileName = "file_";
    Path baseDir = new Path(testRootDir, deletionDir);
    Path fileWithDeletePaths = new Path(testRootDir, "fileWithDeletePaths");
    FSDataOutputStream fsDataOutputStream = lfs.create(fileWithDeletePaths, true);
    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream));
    for (int i = 0; i < 100; i++) {
      Path p = new Path(baseDir, baseFileName + i);
      lfs.create(p);
      br.write(p.toUri().toString());
      br.newLine();
    }
    br.flush(); // flush the file to write the contents
    br.close(); // close the writer
    List<String> finalCommandList = new ArrayList<>();
    finalCommandList.add("-bulkDelete");
    finalCommandList.add("-readFromFile");
    finalCommandList.add(fileWithDeletePaths.toUri().toString());
    finalCommandList.add(baseDir.toUri().toString());
    shell.run(finalCommandList.toArray(new String[0]));
    Assertions.assertThat(lfs.listFiles(baseDir, false).hasNext())
            .as("All the files should have been deleted under the path:" +
                    baseDir).isEqualTo(false);

  }
}
