/**
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

package org.apache.spark.sql.execution.datasources.oap.index;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/** An {@link OutputCommitter} that commits files specified
 * in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}.
 **/
public class OapIndexFileOutputCommitter extends OutputCommitter {
  private static final Log LOG = LogFactory.getLog(OapIndexFileOutputCommitter.class);

  /**
   * Name of directory where pending data is placed.  Data that has not been
   * committed yet.
   */
  private String pendingDirName;
  public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
  public static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
          "mapreduce.fileoutputcommitter.marksuccessfuljobs";
  // Number of attempts when failure happens in commit job
  public static final String FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS =
          "mapreduce.fileoutputcommitter.failures.attempts";

  // default value to be 1 to keep consistent with previous behavior
  public static final int FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS_DEFAULT = 1;
  private Path outputPath = null;
  private Path workPath = null;
  private int algorithmVersion;

  /**
   * Create a file output committer
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a noop.
   * @param context the task's context
   * @throws IOException
   */
  public OapIndexFileOutputCommitter(
      Path outputPath,
      TaskAttemptContext context,
      String pendingDirName,
      int algorithmVersion) throws IOException {
    this(outputPath, (JobContext)context, pendingDirName, algorithmVersion);
    if (outputPath != null) {
      workPath = getTaskAttemptPath(context, outputPath);
    }
  }

  /**
   * Create a file output committer
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a noop.
   * @param context the task's context
   * @throws IOException
   */
  @Private
  public OapIndexFileOutputCommitter(
      Path outputPath,
      JobContext context,
      String pendingDirName,
      int algorithmVersion) throws IOException {
    this.pendingDirName = pendingDirName;
    this.algorithmVersion = algorithmVersion;
    LOG.info("File Output Committer Algorithm version is " + algorithmVersion);
    if (algorithmVersion != 1 && algorithmVersion != 2) {
      throw new IOException("Only 1 or 2 algorithm version is supported");
    }
    if (outputPath != null) {
      FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
      this.outputPath = fs.makeQualified(outputPath);
    }
  }

  /**
   * @return the path where final output of the job should be placed.  This
   * could also be considered the committed application attempt path.
   */
  private Path getOutputPath() {
    return this.outputPath;
  }

  /**
   * @return true if we have an output path set, else false.
   */
  private boolean hasOutputPath() {
    return this.outputPath != null;
  }

  /**
   * @return the path where the output of pending job attempts are
   * stored.
   */
  private Path getPendingJobAttemptsPath() {
    return getPendingJobAttemptsPath(getOutputPath());
  }

  /**
   * Get the location of pending job attempts.
   * @param out the base output directory.
   * @return the location of pending job attempts.
   */
  private Path getPendingJobAttemptsPath(Path out) {
    return new Path(out, pendingDirName);
  }

  /**
   * Get the Application Attempt Id for this job
   * @param context the context to look in
   * @return the Application Attempt Id for a given job.
   */
  private static int getAppAttemptId(JobContext context) {
    return context.getConfiguration().getInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param context the context of the job.  This is used to get the
   * application attempt id.
   * @return the path to store job attempt data.
   */
  public Path getJobAttemptPath(JobContext context) {
    return getJobAttemptPath(context, getOutputPath());
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param context the context of the job.  This is used to get the
   * application attempt id.
   * @param out the output path to place these in.
   * @return the path to store job attempt data.
   */
  public Path getJobAttemptPath(JobContext context, Path out) {
    return getJobAttemptPath(getAppAttemptId(context), out);
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  protected Path getJobAttemptPath(int appAttemptId) {
    return getJobAttemptPath(appAttemptId, getOutputPath());
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  private Path getJobAttemptPath(int appAttemptId, Path out) {
    return new Path(getPendingJobAttemptsPath(out), String.valueOf(appAttemptId));
  }

  /**
   * Compute the path where the output of pending task attempts are stored.
   * @param context the context of the job with pending tasks.
   * @return the path where the output of pending task attempts are stored.
   */
  private Path getPendingTaskAttemptsPath(JobContext context) {
    return getPendingTaskAttemptsPath(context, getOutputPath());
  }

  /**
   * Compute the path where the output of pending task attempts are stored.
   * @param context the context of the job with pending tasks.
   * @return the path where the output of pending task attempts are stored.
   */
  private Path getPendingTaskAttemptsPath(JobContext context, Path out) {
    return new Path(getJobAttemptPath(context, out), pendingDirName);
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   *
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return new Path(getPendingTaskAttemptsPath(context),
            String.valueOf(context.getTaskAttemptID()));
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   *
   * @param context the context of the task attempt.
   * @param out The output path to put things in.
   * @return the path where a task attempt should be stored.
   */
  public Path getTaskAttemptPath(TaskAttemptContext context, Path out) {
    return new Path(getPendingTaskAttemptsPath(context, out),
      String.valueOf(context.getTaskAttemptID()));
  }

  /**
   * Compute the path where the output of a committed task is stored until
   * the entire job is committed.
   * @param context the context of the task attempt
   * @return the path where the output of a committed task is stored until
   * the entire job is committed.
   */
  public Path getCommittedTaskPath(TaskAttemptContext context) {
    return getCommittedTaskPath(getAppAttemptId(context), context);
  }

  public Path getCommittedTaskPath(TaskAttemptContext context, Path out) {
    return getCommittedTaskPath(getAppAttemptId(context), context, out);
  }

  /**
   * Compute the path where the output of a committed task is stored until the
   * entire job is committed for a specific application attempt.
   * @param appAttemptId the id of the application attempt to use
   * @param context the context of any task.
   * @return the path where the output of a committed task is stored.
   */
  protected Path getCommittedTaskPath(int appAttemptId, TaskAttemptContext context) {
    return new Path(getJobAttemptPath(appAttemptId),
      String.valueOf(context.getTaskAttemptID().getTaskID()));
  }

  private Path getCommittedTaskPath(int appAttemptId, TaskAttemptContext context, Path out) {
    return new Path(getJobAttemptPath(appAttemptId, out),
      String.valueOf(context.getTaskAttemptID().getTaskID()));
  }

  private class CommittedTaskFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return !pendingDirName.equals(path.getName());
    }
  }

  /**
   * Get a list of all paths where output from committed tasks are stored.
   * @param context the context of the current job
   * @return the list of these Paths/FileStatuses.
   * @throws IOException
   */
  private FileStatus[] getAllCommittedTaskPaths(JobContext context) throws IOException {
    Path jobAttemptPath = getJobAttemptPath(context);
    FileSystem fs = jobAttemptPath.getFileSystem(context.getConfiguration());
    return fs.listStatus(jobAttemptPath, new CommittedTaskFilter());
  }

  /**
   * Get the directory that the task should write results into.
   * @return the work directory
   * @throws IOException
   */
  public Path getWorkPath() throws IOException {
    return workPath;
  }

  /**
   * Create the temporary directory that is the root of all of the task
   * work directories.
   * @param context the job's context
   */
  public void setupJob(JobContext context) throws IOException {
    if (hasOutputPath()) {
      Path jobAttemptPath = getJobAttemptPath(context);
      FileSystem fs = jobAttemptPath.getFileSystem(context.getConfiguration());
      if (!fs.mkdirs(jobAttemptPath)) {
        LOG.error("Mkdirs failed to create " + jobAttemptPath);
      }
    } else {
      LOG.warn("Output Path is null in setupJob()");
    }
  }

  /**
   * The job has completed, so do works in commitJobInternal().
   * Could retry on failure if using algorithm 2.
   * @param context the job's context
   */
  public void commitJob(JobContext context) throws IOException {
    int maxAttemptsOnFailure = isCommitJobRepeatable(context) ?
      context.getConfiguration().getInt(FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS,
        FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS_DEFAULT) : 1;
    int attempt = 0;
    boolean jobCommitNotFinished = true;
    while (jobCommitNotFinished) {
      try {
        commitJobInternal(context);
        jobCommitNotFinished = false;
      } catch (Exception e) {
        if (++attempt >= maxAttemptsOnFailure) {
          throw e;
        } else {
          LOG.warn("Exception get thrown in job commit, retry (" + attempt + ") time.", e);
        }
      }
    }
  }

  /**
   * The job has completed, so do following commit job, include:
   * Move all committed tasks to the final output dir (algorithm 1 only).
   * Delete the temporary directory, including all of the work directories.
   * Create a _SUCCESS file to make it as successful.
   * @param context the job's context
   */
  protected void commitJobInternal(JobContext context) throws IOException {
    if (hasOutputPath()) {
      Path finalOutput = getOutputPath();
      FileSystem fs = finalOutput.getFileSystem(context.getConfiguration());

      if (algorithmVersion == 1) {
        for (FileStatus stat: getAllCommittedTaskPaths(context)) {
          mergePaths(fs, stat, finalOutput);
        }
      }

      // delete the _temporary folder and create a _done file in the o/p folder
      cleanupJob(context);
      // True if the job requires output.dir marked on successful job.
      // Note that by default it is set to true.
      if (context.getConfiguration().getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
        Path markerPath = new Path(outputPath, SUCCEEDED_FILE_NAME);
        // If job commit is repeatable and previous/another AM could write
        // mark file already, we need to set overwritten to be true explicitly
        // in case other FS implementations don't overwritten by default.
        if (isCommitJobRepeatable(context)) {
          fs.create(markerPath, true).close();
        } else {
          fs.create(markerPath).close();
        }
      }
    } else {
      LOG.warn("Output Path is null in commitJob()");
    }
  }

  /**
   * Merge two paths together.  Anything in from will be moved into to, if there
   * are any name conflicts while merging the files or directories in from win.
   * @param fs the File System to use
   * @param from the path data is coming from.
   * @param to the path data is going to.
   * @throws IOException on any error
   */
  private void mergePaths(FileSystem fs, final FileStatus from, final Path to) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Merging data from " + from + " to " + to);
    }
    FileStatus toStat;
    try {
      toStat = fs.getFileStatus(to);
    } catch (FileNotFoundException fnfe) {
      toStat = null;
    }

    if (from.isFile()) {
      if (toStat != null) {
        if (!fs.delete(to, true)) {
          throw new IOException("Failed to delete " + to);
        }
      }

      if (!fs.rename(from.getPath(), to)) {
        throw new IOException("Failed to rename " + from + " to " + to);
      }
    } else if (from.isDirectory()) {
      if (toStat != null) {
        if (!toStat.isDirectory()) {
          if (!fs.delete(to, true)) {
            throw new IOException("Failed to delete " + to);
          }
          renameOrMerge(fs, from, to);
        } else {
          //It is a directory so merge everything in the directories
          for (FileStatus subFrom : fs.listStatus(from.getPath())) {
            Path subTo = new Path(to, subFrom.getPath().getName());
            mergePaths(fs, subFrom, subTo);
          }
        }
      } else {
        renameOrMerge(fs, from, to);
      }
    }
  }

  private void renameOrMerge(FileSystem fs, FileStatus from, Path to) throws IOException {
    if (algorithmVersion == 1) {
      if (!fs.rename(from.getPath(), to)) {
        throw new IOException("Failed to rename " + from + " to " + to);
      }
    } else {
      fs.mkdirs(to);
      for (FileStatus subFrom : fs.listStatus(from.getPath())) {
        Path subTo = new Path(to, subFrom.getPath().getName());
        mergePaths(fs, subFrom, subTo);
      }
    }
  }

  @Override
  @Deprecated
  public void cleanupJob(JobContext context) throws IOException {
    if (hasOutputPath()) {
      Path pendingJobAttemptsPath = getPendingJobAttemptsPath();
      FileSystem fs = pendingJobAttemptsPath.getFileSystem(context.getConfiguration());
      // if job allow repeatable commit and pendingJobAttemptsPath could be
      // deleted by previous AM, we should tolerate FileNotFoundException in
      // this case.
      try {
        fs.delete(pendingJobAttemptsPath, true);
      } catch (FileNotFoundException e) {
        if (!isCommitJobRepeatable(context)) {
          throw e;
        }
      }
    } else {
      LOG.warn("Output Path is null in cleanupJob()");
    }
  }

  /**
   * Delete the temporary directory, including all of the work directories.
   * @param context the job's context
   */
  @Override
  public void abortJob(JobContext context, JobStatus.State state) throws IOException {
    // delete the _temporary folder
    cleanupJob(context);
  }

  /**
   * No task setup required.
   */
  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    // FileOutputCommitter's setupTask doesn't do anything. Because the
    // temporary task directory is created on demand when the
    // task is writing.
  }

  /**
   * Move the files from the work directory to the job output directory
   * @param context the task context
   */
  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    commitTask(context, null);
  }

  @Private
  public void commitTask(TaskAttemptContext context, Path taskAttemptPath) throws IOException {
    TaskAttemptID attemptId = context.getTaskAttemptID();
    if (hasOutputPath()) {
      context.progress();
      if(taskAttemptPath == null) {
        taskAttemptPath = getTaskAttemptPath(context);
      }
      FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());
      FileStatus taskAttemptDirStatus;
      try {
        taskAttemptDirStatus = fs.getFileStatus(taskAttemptPath);
      } catch (FileNotFoundException e) {
        taskAttemptDirStatus = null;
      }

      if (taskAttemptDirStatus != null) {
        if (algorithmVersion == 1) {
          Path committedTaskPath = getCommittedTaskPath(context);
          if (fs.exists(committedTaskPath)) {
            if (!fs.delete(committedTaskPath, true)) {
              throw new IOException("Could not delete " + committedTaskPath);
            }
          }
          if (!fs.rename(taskAttemptPath, committedTaskPath)) {
            throw new IOException("Could not rename " + taskAttemptPath + " to "
              + committedTaskPath);
          }
          LOG.info("Saved output of task '" + attemptId + "' to " + committedTaskPath);
        } else {
          // directly merge everything from taskAttemptPath to output directory
          mergePaths(fs, taskAttemptDirStatus, outputPath);
          LOG.info("Saved output of task '" + attemptId + "' to " + outputPath);
        }
      } else {
        LOG.warn("No Output found for " + attemptId);
      }
    } else {
      LOG.warn("Output Path is null in commitTask()");
    }
  }

  /**
   * Delete the work directory
   * @throws IOException
   */
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    abortTask(context, null);
  }

  @Private
  public void abortTask(TaskAttemptContext context, Path taskAttemptPath) throws IOException {
    if (hasOutputPath()) {
      context.progress();
      if(taskAttemptPath == null) {
        taskAttemptPath = getTaskAttemptPath(context);
      }
      FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());
      if(!fs.delete(taskAttemptPath, true)) {
        LOG.warn("Could not delete "+taskAttemptPath);
      }
    } else {
      LOG.warn("Output Path is null in abortTask()");
    }
  }

  /**
   * Did this task write any files in the work directory?
   * @param context the task's context
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    return needsTaskCommit(context, null);
  }

  @Private
  public boolean needsTaskCommit(TaskAttemptContext context, Path taskAttemptPath)
    throws IOException {
    if(hasOutputPath()) {
      if(taskAttemptPath == null) {
        taskAttemptPath = getTaskAttemptPath(context);
      }
      FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());
      return fs.exists(taskAttemptPath);
    }
    return false;
  }

  @Override
  @Deprecated
  public boolean isRecoverySupported() {
    return true;
  }

  public boolean isCommitJobRepeatable(JobContext context) throws IOException {
    return algorithmVersion == 2;
  }

  @Override
  public void recoverTask(TaskAttemptContext context) throws IOException {
    if(hasOutputPath()) {
      context.progress();
      TaskAttemptID attemptId = context.getTaskAttemptID();
      int previousAttempt = getAppAttemptId(context) - 1;
      if (previousAttempt < 0) {
        throw new IOException("Cannot recover task output for first attempt...");
      }

      Path previousCommittedTaskPath = getCommittedTaskPath(previousAttempt, context);
      FileSystem fs = previousCommittedTaskPath.getFileSystem(context.getConfiguration());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to recover task from " + previousCommittedTaskPath);
      }
      if (algorithmVersion == 1) {
        if (fs.exists(previousCommittedTaskPath)) {
          Path committedTaskPath = getCommittedTaskPath(context);
          if (fs.exists(committedTaskPath)) {
            if (!fs.delete(committedTaskPath, true)) {
              throw new IOException("Could not delete "+committedTaskPath);
            }
          }
          //Rename can fail if the parent directory does not yet exist.
          Path committedParent = committedTaskPath.getParent();
          fs.mkdirs(committedParent);
          if (!fs.rename(previousCommittedTaskPath, committedTaskPath)) {
            throw new IOException("Could not rename " + previousCommittedTaskPath +
              " to " + committedTaskPath);
          }
        } else {
          LOG.warn(attemptId+" had no output to recover.");
        }
      } else {
        // essentially a no-op, but for backwards compatibility
        // after upgrade to the new fileOutputCommitter,
        // check if there are any output left in committedTaskPath
        if (fs.exists(previousCommittedTaskPath)) {
          LOG.info("Recovering task for upgrading scenario, moving files from "
            + previousCommittedTaskPath + " to " + outputPath);
          FileStatus from = fs.getFileStatus(previousCommittedTaskPath);
          mergePaths(fs, from, outputPath);
        }
        LOG.info("Done recovering task " + attemptId);
      }
    } else {
      LOG.warn("Output Path is null in recoverTask()");
    }
  }
}


