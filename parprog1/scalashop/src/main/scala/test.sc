val numTasks = 4
val size = 20
val rate = if (numTasks < size) size/numTasks else 1

val r = size % numTasks


val a = (0 to size).by(rate)//.updated(numTasks, 4)
val a1 = a.dropRight(1).:+(size)
val b = (0 to size).by(rate).tail.reverse.
  ++:(if (r == 0) List() else List(size)).reverse

a.zip(b)

a1.zip(a1.tail)

val aa = (0 until size).by(rate)
a