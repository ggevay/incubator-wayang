package org.emmalanguage.compiler.lang

import org.emmalanguage.api.DataBag
import org.emmalanguage.api.alg.Count
import org.emmalanguage.api.backend.LocalOps.cross
import org.emmalanguage.compiler.BaseCompilerSpec

class WrapScalarsSpec extends BaseCompilerSpec {

  import compiler._
  import u.reify

  val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.anf,
      Core.unnest
    ).compose(_.tree)

  def applyXfrm(xfrm: Xfrm): u.Expr[Any] => u.Tree = {
    pipeline(typeCheck = true)(
      Core.lnf,
      xfrm.timed,
      Core.unnest
    ).compose(_.tree)
  }

  class TestInt(var v: Int) {
    def addd(u: Int, w: Int, x: Int)(m: Int, n: Int)(s: Int, t: Int) : Int =
      this.v + u + w + x + m + n + s + t

    def add1() : Unit = { v = v + 1 }
  }

  def add1(x: Int) : Int = x + 1
  def str(x: Int) : String = x.toString
  def add(x: Int, y: Int) : Int = x + y
  def add(u: Int, v: Int, w: Int, x: Int, y: Int, z: Int)(m: Int, n: Int)(s: Int, t: Int) : Int =
    u + v + w + x + y + z + m + n + s + t
  
  "normalization" - {

    "ValDef only" in {
      val inp = reify {
        val a = 1
      }
      val exp = reify {
        val a = SingletonBagOps.singSrc(() => {
          val tmp = 1; tmp
        })
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "replace refs on valdef rhs" in {
      val inp = reify {
        val a = 1; val b = a; val c = a; b
      }
      val exp = reify {
        val a = SingletonBagOps.singSrc(() => {
          val tmp = 1; tmp
        });
        val b = a;
        val c = a;
        b
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "ValDef only, SingSrc rhs" in {
      val inp = reify {
        val a = 1;
        val b = SingletonBagOps.singSrc(() => {
          val tmp = 2; tmp
        })
      }
      val exp = reify {
        val a = SingletonBagOps.singSrc(() => {
          val tmp = 1; tmp
        });
        val b = SingletonBagOps.singSrc(() => {
          val tmp = 2; tmp
        })
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "ValDef only, DataBag rhs" in {
      val inp = reify {
        val a = 1
        val b = DataBag(Seq(2))
      }
      val exp = reify {
        val a = SingletonBagOps.singSrc(() => {
          val tmp = 1; tmp
        })
        val s = SingletonBagOps.singSrc(() => {
          val tmp = Seq(2); tmp
        })
        val sb = SingletonBagOps.fromSingSrcApply(s)
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "ValDef only, DataBag rhs 2" in {
      val inp = reify {
        val fun = add1(2)
        val s = Seq(fun)
        val b = DataBag(s)
      }
      val exp = reify {
        val lbdaFun = () => {
          val tmp = add1(2); tmp
        }
        val dbFun = SingletonBagOps.singSrc(lbdaFun)
        val dbs = dbFun.map(e => Seq(e))
        val res = SingletonBagOps.fromSingSrcApply(dbs)
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "replace refs simple" in {
      val inp = reify {
        val a = 1; a
      }
      val exp = reify {
        val a = SingletonBagOps.singSrc(() => {
          val tmp = 1; tmp
        }); a
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method one argument" in {
      val inp = reify {
        val a = 1;
        val b = add1(a);
        b
      }
      val exp = reify {
        val a = SingletonBagOps.singSrc(() => {
          val tmp = 1; tmp
        });
        val b = a.map(e => add1(e));
        b
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method one argument 2" in {
      val inp = reify {
        val a = new TestInt(1);
        val b = a.add1()
        a
      }
      val exp = reify {
        val a = SingletonBagOps.singSrc(() => {
          val tmp = new TestInt(1); tmp
        });
        val b = a.map((e: TestInt) => e.add1());
        a
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method fold1" in {
      val inp = reify {
        val s = Seq(1)
        val b = DataBag(s)
        val count = Count[Int](_ => true)
        val c: Long = b.fold(count)
        c
      }
      val exp = reify {
        val a = SingletonBagOps.singSrc(() => { val tmp = Seq(1); tmp })
        val b: DataBag[Int] = SingletonBagOps.fromSingSrcApply(a)
        val count = Count[Int](_ => true)
        val c: DataBag[Long] = SingletonBagOps.fold1[Int, Long](b, count)
        c
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method fold2" in {
      val inp = reify {
        val s = Seq(1)
        val b = DataBag(s)
        val c: Int = b.fold(0)(i => i, (a, b) => a + b)
        c
      }
      val exp = reify {
        val s = SingletonBagOps.singSrc(() => { val tmp = Seq(1); tmp })
        val b: DataBag[Int] = SingletonBagOps.fromSingSrcApply(s)
        val c: DataBag[Int] = SingletonBagOps.fold2[Int, Int](b, 0, (i: Int) => i, (a, b) => a + b)
        c
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method collect" in {
      val inp = reify {
        val s = Seq(1)
        val b = DataBag(s)
        val c = b.collect()
        c
      }
      val exp = reify {
        val s = SingletonBagOps.singSrc(() => { val tmp = Seq(1); tmp })
        val b: DataBag[Int] = SingletonBagOps.fromSingSrcApply(s)
        val c = SingletonBagOps.collect(b)
        c
      }
      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method one argument typechange" in {
      val inp = reify {
        val a = 1;
        val b = str(a);
        b
      }
      val exp = reify {
        val a = SingletonBagOps.singSrc(() => {
          val tmp = 1; tmp
        });
        val b = a.map(e => str(e));
        b
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method two arguments no constant" in {
      val inp = reify {
        val a = 1
        val b = 2
        val c = add(a, b)
      }
      val exp = reify {
        val a = SingletonBagOps.singSrc(() => {
          val tmp = 1; tmp
        })
        val b = SingletonBagOps.singSrc(() => {
          val tmp = 2; tmp
        })
        val c = cross(a, b).map((t: (Int, Int)) => add(t._1, t._2))
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method two arguments with constants" in {
      val inp = reify {
        val a = 1
        val b = 2
        val c = add(3, 4, a, 5, 6, 7)(8, 9)(10, b)
      }
      val exp = reify {
        val a = SingletonBagOps.singSrc(() => {
          val tmp = 1; tmp
        })
        val b = SingletonBagOps.singSrc(() => {
          val tmp = 2; tmp
        })
        val c = cross(a, b).map((t: (Int, Int)) => add(3, 4, t._1, 5, 6, 7)(8, 9)(10, t._2))
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method two arguments 2" in {
      val inp = reify {
        val a = new TestInt(1)
        val b = 2
        val c = a.addd(1, b, 3)(4, 5)(6, 7)
      }
      val exp = reify {
        val a = SingletonBagOps.singSrc(() => {
          val tmp = new TestInt(1); tmp
        })
        val b = SingletonBagOps.singSrc(() => {
          val tmp = 2; tmp
        })
        val c = cross(a, b).map((t: (TestInt, Int)) => t._1.addd(1, t._2, 3)(4, 5)(6, 7))
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method three arguments with constants" in {
      val inp = reify {
        val a = 1
        val b = 2
        val c = 3
        val d = add(3, 4, a, 5, 6, 7)(c, 9)(10, b)
      }
      val exp = reify {
        val a = SingletonBagOps.singSrc(() => {
          val tmp = 1; tmp
        })
        val b = SingletonBagOps.singSrc(() => {
          val tmp = 2; tmp
        })
        val c = SingletonBagOps.singSrc(() => {
          val tmp = 3; tmp
        })
        val d = SingletonBagOps.cross3(a, c, b).map((t: (Int, Int, Int)) => add(3, 4, t._1, 5, 6, 7)(t._2, 9)(10, t._3))
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method three arguments 2" in {
      val inp = reify {
        val a = new TestInt(1)
        val b = 2
        val c = 3
        val d = a.addd(1, b, 3)(4, c)(6, 7)
      }
      val exp = reify {
        val a = SingletonBagOps.singSrc(() => {
          val tmp = new TestInt(1); tmp
        })
        val b = SingletonBagOps.singSrc(() => {
          val tmp = 2; tmp
        })
        val c = SingletonBagOps.singSrc(() => {
          val tmp = 3; tmp
        })
        val d = SingletonBagOps.cross3(a, b, c).map((t: (TestInt, Int, Int)) => t._1.addd(1, t._2, 3)(4, t._3)(6, 7))
      }

      applyXfrm(WrapScalars.transform)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }
  }
}
