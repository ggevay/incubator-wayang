package org.emmalanguage.lib

import org.emmalanguage.api.emma

@emma.lib
class TestClass(val f1: Int) {

  def inc(): org.emmalanguage.lib.TestClass = { // todo: Why do I need to qualify it? Does the old inlining also have this requirement?
    new org.emmalanguage.lib.TestClass(f1 + 1)
  }

}
