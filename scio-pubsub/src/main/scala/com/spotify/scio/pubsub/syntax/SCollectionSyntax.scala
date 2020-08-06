/*
 * Copyright 2020 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.pubsub.syntax

import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.Coder
import com.spotify.scio.pubsub.PubsubIO
import com.spotify.scio.io.ClosedTap
import scala.reflect.ClassTag

trait SCollectionSyntax {
  implicit class SCollectionPubsubOps[T](private val coll: SCollection[T]) {

    /**
     * Save this SCollection as a Pub/Sub topic.
     * @group output
     */
    def saveAsPubsub(
      topic: String,
      idAttribute: String = null,
      timestampAttribute: String = null,
      maxBatchSize: Option[Int] = None,
      maxBatchBytesSize: Option[Int] = None
    )(implicit ct: ClassTag[T], coder: Coder[T]): ClosedTap[Nothing] = {
      val io = PubsubIO[T](topic, idAttribute, timestampAttribute)
      coll.write(io)(PubsubIO.WriteParam(maxBatchSize, maxBatchBytesSize))
    }

    /**
     * Save this SCollection as a Pub/Sub topic using the given map as message attributes.
     * @group output
     */
    def saveAsPubsubWithAttributes[V: ClassTag: Coder](
      topic: String,
      idAttribute: String = null,
      timestampAttribute: String = null,
      maxBatchSize: Option[Int] = None,
      maxBatchBytesSize: Option[Int] = None
    )(implicit ev: T <:< (V, Map[String, String])): ClosedTap[Nothing] = {
      val io = PubsubIO.withAttributes[V](topic, idAttribute, timestampAttribute)
      coll
        .covary_[(V, Map[String, String])]
        .write(io)(PubsubIO.WriteParam(maxBatchSize, maxBatchBytesSize))
    }
  }

}
