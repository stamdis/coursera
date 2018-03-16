package recfun

object Main {
  def main(args: Array[String]) {
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(pascal(col, row) + " ")
      println()
    }
  }

  /**
   * Exercise 1
   */
    def pascal(c: Int, r: Int): Int = {
      if(c == 0 || c == r)
        1
      else
        pascal(c, r-1) + pascal(c-1, r-1)
    }
  
  /**
   * Exercise 2
   */
    def balance(chars: List[Char]): Boolean = {

      def getVal(chars1: List[Char], c: Int): Boolean = {
        if(c < 0)
          false
        else if(chars1.isEmpty && c == 0)
          true
        else if (chars1.isEmpty && c != 0)
          false
        else if(chars1.head == '(')
          getVal(chars1.tail, c+1)
        else if(chars1.head == ')')
          getVal(chars1.tail, c-1)
        else
          getVal(chars1.tail, c)
      }

      getVal(chars,0)
    }
  
  /**
   * Exercise 3
   */
    def countChange(money: Int, coins: List[Int]): Int = {
      if(money == 0)
        1
      else if(money > 0 && !coins.isEmpty)
        countChange(money, coins.tail) + countChange(money-coins.head, coins)
      else
        0
    }
  }
