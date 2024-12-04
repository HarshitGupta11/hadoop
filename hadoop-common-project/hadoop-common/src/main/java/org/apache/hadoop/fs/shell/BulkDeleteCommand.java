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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BulkDeleteCommand extends FsCommand {
    public static void registerCommands(CommandFactory factory) {
        factory.addClass(BulkDeleteCommand.class, "-bulkDelete");
    }

    public static final String name = "bulkDelete";

    public static final String READ_FROM_FILE = "readFromFile";

    public static final String USAGE = "-[ " + READ_FROM_FILE + "] [<file>] [<basePath> <paths>]";

    public static final String DESCRIPTION = "Deletes the set of files under the given path. If a list of paths " +
            "is provided then the paths are deleted directly. User can also point to the file where the paths are" +
            "listed as full object names.";

    private String fileName;

    /*
    Making the class stateful as the PathData initialization for all args is not needed
     */
    LinkedList<String> childArgs;

    protected BulkDeleteCommand() {
        this.childArgs = new LinkedList<>();
    }

    protected BulkDeleteCommand(Configuration conf) {super(conf);}

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
        CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE);
        cf.addOptionWithValue(READ_FROM_FILE);
        cf.parse(args);
        fileName = cf.getOptValue(READ_FROM_FILE);
    }

    @Override
    protected LinkedList<PathData> expandArguments(LinkedList<String> args) throws IOException {
        if(fileName == null && args.size() < 2) {
            throw new IOException("Invalid Number of Arguments. Expected more");
        }
        LinkedList<PathData> pathData = new LinkedList<>();
        pathData.add(new PathData(args.get(0), getConf()));
        args.remove(0);
        this.childArgs = args;
        return pathData;
    }

    @Override
    protected void processArguments(LinkedList<PathData> args) throws IOException {
        PathData basePath = args.get(0);
        out.println("Deleting files under:" + basePath);
        List<Path> pathList = new ArrayList<>();
        if(fileName != null) {
            FileSystem localFile = FileSystem.get(getConf());
            BufferedReader br = new BufferedReader(new InputStreamReader(localFile.open(new Path(fileName))));
            String line;
            while((line = br.readLine()) != null) {
                if(!line.startsWith("#")) {
                    pathList.add(new Path(line));
                }
            }
        } else {
            pathList.addAll(this.childArgs.stream().map(Path::new).collect(Collectors.toList()));
        }
        BulkDelete bulkDelete = basePath.fs.createBulkDelete(basePath.path);
        bulkDelete.bulkDelete(pathList);
    }
}
