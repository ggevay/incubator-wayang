package org.emmalanguage.lib

import org.emmalanguage.api.emma

@emma.lib
class TestClass(val f1: Int) {

  def inc(): TestClass = {
    new TestClass(f1 + 1)
  }

}
