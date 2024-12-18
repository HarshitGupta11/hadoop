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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkDeleteCommand extends FsCommand {

  public static void registerCommands(CommandFactory factory) {
    factory.addClass(BulkDeleteCommand.class, "-bulkDelete");
  }

  private static final Logger LOG = LoggerFactory.getLogger(BulkDeleteCommand.class.getName());

  public static final String NAME = "bulkDelete";

  /**
   * File Name parameter to be specified at command line.
   */
  public static final String READ_FROM_FILE = "readFromFile";

  /**
   * Page size parameter specified at command line.
   */
  public static final String PAGE_SIZE = "pageSize";


  public static final String USAGE = "-[ " + READ_FROM_FILE + "] [<file>] [" + PAGE_SIZE
          + "] [<pageSize>] [<basePath> <paths>]";

  public static final String DESCRIPTION = "Deletes the set of files under the given <path>.\n" +
          "If a list of paths is provided at command line then the paths are deleted directly.\n" +
          "User can also point to the file where the paths are listed as full object names using the \"fileName\"" +
          "parameter. The presence of a file name takes precedence over the list of objects.\n" +
          "Page size refers to the size of each bulk delete batch." +
          "Users can specify the page size using \"pageSize\" command parameter." +
          "Default value is 1.\n";

  private String fileName;

  private int pageSize;

  /*
  Making the class stateful as the PathData initialization for all args is not needed
   */ LinkedList<String> childArgs;

  protected BulkDeleteCommand() {
    this.childArgs = new LinkedList<>();
  }

  protected BulkDeleteCommand(Configuration conf) {
    super(conf);
  }

  /**
   * Processes the command line options and initialize the variables.
   *
   * @param args the command line arguments
   * @throws IOException in case of wrong arguments passed
   */
  @Override
  protected void processOptions(LinkedList<String> args) throws IOException {
    CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE);
    cf.addOptionWithValue(READ_FROM_FILE);
    cf.addOptionWithValue(PAGE_SIZE);
    cf.parse(args);
    fileName = cf.getOptValue(READ_FROM_FILE);
    if (cf.getOptValue(PAGE_SIZE) != null) {
      pageSize = Integer.parseInt(cf.getOptValue(PAGE_SIZE));
    } else {
      pageSize = 1;
    }
  }

  /**
   * Processes the command line arguments and stores the child arguments in a list.
   *
   * @param args strings to expand into {@link PathData} objects
   * @return the base path of the bulk delete command.
   * @throws IOException if the wrong number of arguments specified
   */
  @Override
  protected LinkedList<PathData> expandArguments(LinkedList<String> args) throws IOException {
    if (fileName == null && args.size() < 2) {
      throw new IOException("Invalid Number of Arguments. Expected more");
    }
    LinkedList<PathData> pathData = new LinkedList<>();
    pathData.add(new PathData(args.get(0), getConf()));
    args.remove(0);
    this.childArgs = args;
    return pathData;
  }

  /**
   * Deletes the objects using the bulk delete api.
   *
   * @param bulkDelete Bulkdelete object exposing the API
   * @param paths      list of paths to be deleted in the base path
   * @throws IOException on error in execution of the delete command
   */
  void deleteInBatches(BulkDelete bulkDelete, List<Path> paths) throws IOException {
    Batch<Path> batches = new Batch<>(paths, pageSize);
    while (batches.hasNext()) {
      try {
        List<Map.Entry<Path, String>> result = bulkDelete.bulkDelete(batches.next());
        LOG.debug("Deleted Result:{}", result.toString());
      } catch (IllegalArgumentException e) {
        LOG.error("Caught exception while deleting", e);
      }
    }
  }

  @Override
  protected void processArguments(LinkedList<PathData> args) throws IOException {
    PathData basePath = args.get(0);
    LOG.info("Deleting files under:{}", basePath);
    List<Path> pathList = new ArrayList<>();
    if (fileName != null) {
      LOG.info("Reading from file:{}", fileName);
      FileSystem localFile = FileSystem.get(getConf());
      BufferedReader br = new BufferedReader(new InputStreamReader(
              localFile.open(new Path(fileName)), StandardCharsets.UTF_8));
      String line;
      while ((line = br.readLine()) != null) {
        if (!line.startsWith("#")) {
          pathList.add(new Path(line));
        }
      }
      br.close();
    } else {
      pathList.addAll(this.childArgs.stream().map(Path::new).collect(Collectors.toList()));
    }
    LOG.debug("Deleting:{}", pathList);
    BulkDelete bulkDelete = basePath.fs.createBulkDelete(basePath.path);
    deleteInBatches(bulkDelete, pathList);
  }

  /**
   * Batch class for deleting files in batches, once initialized the inner list can't be modified.
   *
   * @param <T> template type for batches
   */
  static class Batch<T> {
    private final List<T> data;
    private final int batchSize;
    private int currentLocation;

    Batch(List<T> data, int batchSize) {
      this.data = Collections.unmodifiableList(data);
      this.batchSize = batchSize;
      this.currentLocation = 0;
    }

    /**
     * @return If there is a next batch present
     */
    boolean hasNext() {
      return currentLocation < data.size();
    }

    /**
     * @return Compute and return a new batch
     */
    List<T> next() {
      List<T> ret = new ArrayList<>();
      int i = 0;
      while (i < batchSize && currentLocation < data.size()) {
        ret.add(data.get(currentLocation));
        i++;
        currentLocation++;
      }
      return ret;
    }
  }
}
