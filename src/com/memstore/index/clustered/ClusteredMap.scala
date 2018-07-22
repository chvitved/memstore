package com.memstore.index.clustered

import scala.collection.generic._
import scala.collection.mutable.Builder
import annotation.bridge
import scala.collection.immutable.RedBlack
import scala.collection.immutable.SortedMap
import scala.collection.SortedMapLike
import scala.collection.immutable.MapLike
import scala.collection.GenTraversableOnce
import scala.collection.immutable.TreeSet

/**
 * 
 * This is a copy of the scala.collection.immutable.Treemap with a function floor added
 * 
 */

object ClusteredMap extends ImmutableSortedMapFactory[ClusteredMap] {
  def empty[A, B](implicit ord: Ordering[A]) = new ClusteredMap[A, B]()(ord)
  /** $sortedMapCanBuildFromInfo */
  implicit def canBuildFrom[A, B](implicit ord: Ordering[A]): CanBuildFrom[Coll, (A, B), ClusteredMap[A, B]] = new SortedMapCanBuildFrom[A, B]
  private def make[A, B](s: Int, t: RedBlack[A]#Tree[B])(implicit ord: Ordering[A]) = new ClusteredMap[A, B](s, t)(ord)
  
}

class ClusteredMap[A, +B](override val size: Int, t: RedBlack[A]#Tree[B])(implicit val ordering: Ordering[A]) extends RedBlack[A]
     with SortedMap[A, B]
     with SortedMapLike[A, B, ClusteredMap[A, B]]
     with MapLike[A, B, ClusteredMap[A, B]]
     with Serializable {
  
  def floor(key: A): Option[(A,B)] = {
    
    def gt(key: A, node: Option[NonEmpty[B]]) : Boolean = {
      node match {
        case None => true
        case Some(n) => ordering.gt(key, n.key)
      }
    }
    
    def floor(key: A, tree: RedBlack[A]#Tree[B], matchSoFar: Option[NonEmpty[B]]): Option[(A,B)] = {
      tree match {
	      case t: NonEmpty[B] => {
	        if (t.key == key) Some((t.key -> t.value))
	        else if (ordering.lt(t.key, key)) {
	        	if (gt(t.key, matchSoFar)) floor(key, t.right, Some(t))
	          	else floor(key, t.right, matchSoFar)
	        } else {
	          (floor(key, t.left, matchSoFar))
	        } 
	      }
	      case empty => matchSoFar.map(t => (t.key -> t.value))
      }
    }
    
    val res =floor(key, tree, None)
    res
  }


  def isSmaller(x: A, y: A) = ordering.lt(x, y)

  override protected[this] def newBuilder : Builder[(A, B), ClusteredMap[A, B]] =
    ClusteredMap.newBuilder[A, B]

  def this()(implicit ordering: Ordering[A]) = this(0, null)(ordering)

  protected val tree: RedBlack[A]#Tree[B] = if (size == 0) Empty else t

  override def rangeImpl(from : Option[A], until : Option[A]): ClusteredMap[A,B] = {
    val ntree = tree.range(from,until)
    new ClusteredMap[A,B](ntree.count, ntree)
  }

  override def firstKey = t.first
  override def lastKey = t.last
  override def compare(k0: A, k1: A): Int = ordering.compare(k0, k1)

  /** A factory to create empty maps of the same type of keys.
   */
  override def empty: ClusteredMap[A, B] = ClusteredMap.empty[A, B](ordering)

  /** A new ClusteredMap with the entry added is returned,
   *  if key is <em>not</em> in the ClusteredMap, otherwise
   *  the key is updated with the new entry.
   *
   *  @tparam B1     type of the value of the new binding which is a supertype of `B`
   *  @param key     the key that should be updated
   *  @param value   the value to be associated with `key`
   *  @return        a new $coll with the updated binding
   */
  override def updated [B1 >: B](key: A, value: B1): ClusteredMap[A, B1] = {
    val newsize = if (tree.lookup(key).isEmpty) size + 1 else size
    ClusteredMap.make(newsize, tree.update(key, value))
  }

  /** Add a key/value pair to this map.
   *  @tparam   B1   type of the value of the new binding, a supertype of `B`
   *  @param    kv   the key/value pair
   *  @return        A new $coll with the new binding added to this map
   */
  override def + [B1 >: B] (kv: (A, B1)): ClusteredMap[A, B1] = updated(kv._1, kv._2)

  /** Adds two or more elements to this collection and returns
   *  either the collection itself (if it is mutable), or a new collection
   *  with the added elements.
   *
   *  @tparam B1   type of the values of the new bindings, a supertype of `B`
   *  @param elem1 the first element to add.
   *  @param elem2 the second element to add.
   *  @param elems the remaining elements to add.
   *  @return      a new $coll with the updated bindings
   */
  override def + [B1 >: B] (elem1: (A, B1), elem2: (A, B1), elems: (A, B1) *): ClusteredMap[A, B1] =
    this + elem1 + elem2 ++ elems

  /** Adds a number of elements provided by a traversable object
   *  and returns a new collection with the added elements.
   *
   *  @param xs     the traversable object.
   */
  override def ++[B1 >: B] (xs: GenTraversableOnce[(A, B1)]): ClusteredMap[A, B1] =
    ((repr: ClusteredMap[A, B1]) /: xs.seq) (_ + _)

  def ++[B1 >: B] (xs: TraversableOnce[(A, B1)]): ClusteredMap[A, B1] = ++(xs: GenTraversableOnce[(A, B1)])

  /** A new ClusteredMap with the entry added is returned,
   *  assuming that key is <em>not</em> in the ClusteredMap.
   *
   *  @tparam B1    type of the values of the new bindings, a supertype of `B`
   *  @param key    the key to be inserted
   *  @param value  the value to be associated with `key`
   *  @return       a new $coll with the inserted binding, if it wasn't present in the map
   */
  def insert [B1 >: B](key: A, value: B1): ClusteredMap[A, B1] = {
    assert(tree.lookup(key).isEmpty)
    ClusteredMap.make(size + 1, tree.update(key, value))
  }

  def - (key:A): ClusteredMap[A, B] =
    if (tree.lookup(key).isEmpty) this
    else if (size == 1) empty
    else ClusteredMap.make(size - 1, tree.delete(key))

  /** Check if this map maps `key` to a value and return the
   *  value if it exists.
   *
   *  @param  key     the key of the mapping of interest
   *  @return         the value of the mapping, if it exists
   */
  override def get(key: A): Option[B] = tree.lookup(key) match {
    case n: NonEmpty[b] => Some(n.value)
    case _ => None
  }

  /** Creates a new iterator over all elements contained in this
   *  object.
   *
   *  @return the new iterator
   */
  def iterator: Iterator[(A, B)] = tree.toStream.iterator

  override def toStream: Stream[(A, B)] = tree.toStream

  override def foreach[U](f : ((A,B)) =>  U) = tree foreach { case (x, y) => f(x, y) }
}