package ca.mcit.bigdata.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait HadoopClient {

  val conf=new Configuration()
  val hadoopconfDir = "C:/Users/vamsh/Downloads/cc"
  conf.addResource(new Path(s"$hadoopconfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopconfDir/hdfs-site.xml"))
  val fs: FileSystem = FileSystem.get(conf)


}
