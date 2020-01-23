package com.spotify.scio.repl

import kantan.csv.CsvReader
import com.spotify.scio.repl.compat.CsvReaderOps

package object compat {

  implicit def csvReaderOps[A](reader: CsvReader[A]): CsvReaderOps[A] = new CsvReaderOps(reader)

}
