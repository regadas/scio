package scala.concurrent

object ExecutionContexts {
  val parasitic = new ExecutionContextExecutor with BatchingExecutor {
    final override def unbatchedExecute(runnable: Runnable): Unit = runnable.run()
    final override def reportFailure(t: Throwable): Unit = ExecutionContext.defaultReporter(t)
  }
}
