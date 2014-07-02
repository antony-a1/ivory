package com.ambiata.ivory.data

import com.ambiata.mundane.control._
import scalaz._

// TODO Move to mundane?
object ResultTUtil {

  def retry[F[+_] : Monad, A](times: Int)(f: => ResultT[F, A]): ResultT[F, A] =
    if (times <= 0) f
    else {
      // TODO https://github.com/ambiata/mundane/pull/21
      // (f ||| retry(times - 1)(f))
      import Scalaz._
      ResultT(f.run.flatMap { result => if (result.isOk) Monad[F].point(result) else retry(times - 1)(f).run})
    }
}
