/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.transforms

import java.lang
import java.util.function.{Function => JFunction}

import scala.jdk.CollectionConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContexts

/** A [[FutureHandlers.Base]] implementation for Scala [[Future]]. */
trait ScalaFutureHandlers[T] extends FutureHandlers.Base[Future[T], T] {
  import ScalaFutureHandlers.ec

  override def waitForFutures(futures: lang.Iterable[Future[T]]): Unit = {
    Await.ready(Future.sequence(futures.asScala), Duration.Inf)
    ()
  }

  override def addCallback(
    future: Future[T],
    onSuccess: JFunction[T, Void],
    onFailure: JFunction[Throwable, Void]
  ): Future[T] =
    future.andThen {
      case Failure(exception) => onFailure(exception)
      case Success(value) =>
        try onSuccess(value)
        catch { case exp: Throwable => onFailure(exp) }
    }
}

object ScalaFutureHandlers {
  implicit private val ec: ExecutionContext = ExecutionContexts.parasitic
}
