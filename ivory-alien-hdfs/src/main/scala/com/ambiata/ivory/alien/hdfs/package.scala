package com.ambiata.ivory.alien

import com.amazonaws.services.s3.AmazonS3Client

package object hdfs {
  type HdfsS3Action[A] = HdfsAwsAction[A, AmazonS3Client]
}
