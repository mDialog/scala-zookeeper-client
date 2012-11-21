package com.mdialog.zookeeper

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.immutable.Set
import org.apache.zookeeper.{ CreateMode, KeeperException, Watcher, WatchedEvent, ZooKeeper }
import org.apache.zookeeper.data.{ ACL, Stat, Id }
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.slf4j.{ Logger, LoggerFactory }
import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.util.concurrent.atomic.AtomicBoolean

class ZooKeeperClient(servers: String, sessionTimeout: Int, basePath: String,
    uponNewSession: Option[ZooKeeperClient ⇒ Unit],
    uponSessionExpiry: Option[ZooKeeperClient ⇒ Unit]) {
  private val log = LoggerFactory.getLogger(this.getClass)
  @volatile private var zk: ZooKeeper = null
  @volatile private var disconnected: Boolean = true
  @volatile private var newSession: Boolean = true
  connect()

  def this(servers: String, sessionTimeout: Int, basePath: String) =
    this(servers, sessionTimeout, basePath, None, None)

  def this(servers: String, sessionTimeout: Int, basePath: String, uponNewSession: ZooKeeperClient ⇒ Unit) =
    this(servers, sessionTimeout, basePath, Some(uponNewSession), None)

  def this(servers: String, sessionTimeout: Int, basePath: String, uponNewSession: ZooKeeperClient ⇒ Unit, uponSessionExpiry: ZooKeeperClient ⇒ Unit) =
    this(servers, sessionTimeout, basePath, Some(uponNewSession), Some(uponSessionExpiry))

  def this(servers: String) =
    this(servers, 3000, "", None, None)

  def this(servers: String, uponNewSession: ZooKeeperClient ⇒ Unit) =
    this(servers, 3000, "", Some(uponNewSession), None)

  def this(servers: String, uponNewSession: ZooKeeperClient ⇒ Unit, uponSessionExpiry: ZooKeeperClient ⇒ Unit) =
    this(servers, 3000, "", Some(uponNewSession), Some(uponSessionExpiry))

  def getHandle(): ZooKeeper = zk

  /**
   * connect() attaches to the remote zookeeper and sets an instance variable.
   */
  private def connect() {
    val connectionLatch = new CountDownLatch(1)
    val assignLatch = new CountDownLatch(1)
    if (zk != null) {
      zk.close()
      zk = null
    }
    newSession = true

    zk = new ZooKeeper(servers, sessionTimeout,
      new Watcher {
        def process(event: WatchedEvent) {
          sessionEvent(assignLatch, connectionLatch, event)
        }
      })
    assignLatch.countDown()
    log.info("Attempting to connect to zookeeper servers {}", servers)
    connectionLatch.await()
  }

  def sessionEvent(assignLatch: CountDownLatch, connectionLatch: CountDownLatch, event: WatchedEvent) {
    log.info("Zookeeper event: {}", event)
    assignLatch.await()
    event.getState match {
      case KeeperState.SyncConnected ⇒ {
        disconnected = false
        try {
          if (newSession) {
            uponNewSession.map(fn ⇒ fn(this))
            newSession = false
          }
        } catch {
          case e: Exception ⇒
            log.error("Exception during zookeeper connection established callback", e)
        }
        connectionLatch.countDown()
      }
      case KeeperState.Expired ⇒ {
        // Session was expired; create a new zookeeper connection
        disconnected = true
        uponSessionExpiry.map(fn ⇒ fn(this))
        connect()
      }
      case _ ⇒ // Disconnected -- zookeeper library will handle reconnects
        disconnected = true
    }
  }

  /**
   * Given a string representing a path, return each subpath
   * Ex. subPaths("/a/b/c", "/") == ["/a", "/a/b", "/a/b/c"]
   */
  def subPaths(path: String, sep: Char) = {
    val l = path.split(sep).toList
    val paths = l.tail.foldLeft[List[String]](Nil) { (xs, x) ⇒
      (xs.headOption.getOrElse("") + sep.toString + x) :: xs
    }
    paths.reverse
  }

  private def makeNodePath(path: String) = "%s/%s".format(basePath, path).replaceAll("//", "/")

  def getChildren(path: String): Seq[String] = {
    zk.getChildren(makeNodePath(path), false)
  }

  def close() = zk.close

  def isAlive: Boolean = {
    if (disconnected) return false // nonblocking isAlive

    // If you can get the root, then we're alive.
    val result: Stat = zk.exists("/", false) // do not watch
    result.getVersion >= 0
  }

  def create(path: String, data: Array[Byte], createMode: CreateMode): String = {
    zk.create(makeNodePath(path), data, Ids.OPEN_ACL_UNSAFE, createMode)
  }

  /**
   * ZooKeeper version of mkdir -p
   */
  def createPath(path: String) {
    for (path ← subPaths(makeNodePath(path), '/')) {
      try {
        log.debug("Creating path in createPath: {}", path)
        if (isAlive) {
          zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        } else {
          log.warn("Attempting to createPath while ZooKeeperClient not connected")
        }
      } catch {
        case _: KeeperException.NodeExistsException ⇒ {} // ignore existing nodes
      }
    }
  }

  def get(path: String): Array[Byte] = {
    zk.getData(makeNodePath(path), false, null)
  }

  def set(path: String, data: Array[Byte]) {
    zk.setData(makeNodePath(path), data, -1)
  }

  def delete(path: String) {
    zk.delete(makeNodePath(path), -1)
  }

  /**
   * Delete a node along with all of its children
   */
  def deleteRecursive(path: String) {
    val children = getChildren(path)
    for (node ← children) {
      deleteRecursive(path + '/' + node)
    }
    delete(path)
  }

  /**
   * Watches a node. When the node's data is changed, onDataChanged will be called with the
   * new data value as a byte array. If the node is deleted, onDataChanged will be called with
   * None and will track the node's re-creation with an existence watch.
   */
  def watchNode(node: String, onDataChanged: Option[Array[Byte]] ⇒ Unit) {
    log.debug("Watching node {}", node)
    val path = makeNodePath(node)

    def updateData(fromDeleted: Boolean = false) {
      try {
        val st = new org.apache.zookeeper.data.Stat()
        val dt = {
          if (!fromDeleted)
            zk.getData(path, dataGetter(), st)
          else
            zk.getData(path, false, st)
        }

        onDataChanged(Some(dt))
      } catch {
        case e: KeeperException ⇒ {
          log.warn("Failed to read node {}: {}", path, e)

          if (!fromDeleted)
            deletedData
        }
      }
    }

    def deletedData {
      onDataChanged(None)

      var st: org.apache.zookeeper.data.Stat = null
      var addedWatch = false

      /* retry until it is known a watch has been added */
      while (!addedWatch) {
        try {
          st = zk.exists(path, dataGetter())
          addedWatch = true
        } catch {
          case e: KeeperException ⇒
            Thread.sleep(300)
        }
      }

      if (st != null)
        updateData(true) /* node is no longer deleted */
    }

    def dataGetter() = {
      new Watcher {
        def process(event: WatchedEvent) {
          if (event.getType == EventType.NodeDataChanged || event.getType == EventType.NodeCreated) {
            updateData()
          } else if (event.getType == EventType.NodeDeleted) {
            deletedData
          }
        }
      }
    }

    updateData()
  }

  /**
   * Gets the children for a node (relative path from our basePath), watches
   * for each NodeChildrenChanged event and runs the supplied updateChildren function and
   * re-watches the node's children.
   */
  def watchChildren(node: String, updateChildren: Seq[String] ⇒ Unit) {
    val path = makeNodePath(node)
    val childWatcher = new Watcher {
      def process(event: WatchedEvent) {
        if (event.getType == EventType.NodeChildrenChanged ||
          event.getType == EventType.NodeCreated) {
          watchChildren(node, updateChildren)
        }
      }
    }
    try {
      val children = zk.getChildren(path, childWatcher)
      updateChildren(children)
    } catch {
      case e: KeeperException ⇒ {
        // Node was deleted -- fire a watch on node re-creation
        log.warn("Failed to read node {}: {}", path, e)
        updateChildren(List())
        zk.exists(path, childWatcher)
      }
    }
  }

  /**
   * WARNING: watchMap must be thread-safe. Writing is synchronized on the watchMap. Readers MUST
   * also synchronize on the watchMap for safety.
   */
  def watchChildrenWithData[T](node: String, watchMap: mutable.Map[String, T], deserialize: Array[Byte] ⇒ T) {
    watchChildrenWithData(node, watchMap, deserialize, None)
  }

  /**
   * Watch a set of nodes with an explicit notifier. The notifier will be called whenever
   * the watchMap is modified
   */
  def watchChildrenWithData[T](node: String, watchMap: mutable.Map[String, T],
    deserialize: Array[Byte] ⇒ T, notifier: String ⇒ Unit) {
    watchChildrenWithData(node, watchMap, deserialize, Some(notifier))
  }

  private def watchChildrenWithData[T](node: String, watchMap: mutable.Map[String, T],
    deserialize: Array[Byte] ⇒ T, notifier: Option[String ⇒ Unit]) {
    def nodeChanged(child: String)(childData: Option[Array[Byte]]) {
      childData match {
        case Some(data) ⇒ {
          watchMap.synchronized {
            watchMap(child) = deserialize(data)
          }
          notifier.map(f ⇒ f(child))
        }
        case None ⇒ // deletion handled via parent watch
      }
    }

    def parentWatcher(children: Seq[String]) {
      val childrenSet = Set(children: _*)
      val watchedKeys = Set(watchMap.keySet.toSeq: _*)
      val removedChildren = watchedKeys -- childrenSet
      val addedChildren = childrenSet -- watchedKeys
      watchMap.synchronized {
        // remove deleted children from the watch map
        for (child ← removedChildren) {
          log.debug("Node {}: child {} removed", node, child)
          watchMap -= child
        }
        // add new children to the watch map
        for (child ← addedChildren) {
          // node is added via nodeChanged callback
          log.debug("Node {}: child {} added", node, child)
          watchNode("%s/%s".format(node, child), nodeChanged(child))
        }
      }
      for (child ← removedChildren) {
        notifier.map(f ⇒ f(child))
      }
    }

    watchChildren(node, parentWatcher)
  }
}
