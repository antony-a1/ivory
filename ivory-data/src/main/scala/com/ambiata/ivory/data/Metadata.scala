package com.ambiata.ivory.data

sealed trait Metadata[F[_]] {
  def next: F[Identifier]

}
