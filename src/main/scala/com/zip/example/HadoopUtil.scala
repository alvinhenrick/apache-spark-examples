package com.zip.example

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}

/**
  * Created by shona on 2/24/16.
  */
object HadoopUtil {

  /**
    * Get [[org.apache.hadoop.fs.FileStatus]] objects for all leaf children (files) under the given base path. If the
    * given path points to a file, return a single-element collection containing [[org.apache.hadoop.fs.FileStatus]] of
    * that file.
    */
  def listLeafStatuses(fs: FileSystem, basePath: Path): Seq[FileStatus] = {
    listLeafStatuses(fs, fs.getFileStatus(basePath))
  }

  /**
    * Get [[FileStatus]] objects for all leaf children (files) under the given base path. If the
    * given path points to a file, return a single-element collection containing [[FileStatus]] of
    * that file.
    */
  def listLeafStatuses(fs: FileSystem, baseStatus: FileStatus): Seq[FileStatus] = {
    def recurse(status: FileStatus): Seq[FileStatus] = {
      val (directories, leaves) = fs.listStatus(status.getPath, new PathFilter {
        override def accept(path: Path): Boolean = {
          val name = path.getName
          !fs.isDirectory(path) && name.endsWith("zip")
        }
      }).partition(_.isDirectory)
      leaves ++ directories.flatMap(f => listLeafStatuses(fs, f))
    }

    if (baseStatus.isDirectory) recurse(baseStatus) else Seq(baseStatus)
  }

}
