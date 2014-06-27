package com.ambiata.ivory.api

/**
 * The ivory "nursery" API forms an exported API for "experimental" or
 * "in-flux" components. The goal of these APIs is to eventually
 * move them to the stable ivory API.
 */
object IvoryNursery {
  /**
   * Repository types
   */
  type Repository = com.ambiata.ivory.storage.repository.Repository
  val Repository = com.ambiata.ivory.storage.repository.Repository
}
