package com.ambiata.ivory

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient

package object scoobi {
  type ScoobiS3Action[A] = ScoobiAwsAction[A, AmazonS3Client]
  type ScoobiS3EMRAction[A] = ScoobiAwsAction[A, AmazonS3EMRClient]
  type AmazonEMRClient = AmazonElasticMapReduceClient
  type AmazonS3EMRClient = (AmazonS3Client, AmazonElasticMapReduceClient)
}
