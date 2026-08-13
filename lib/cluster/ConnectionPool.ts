import { EventEmitter } from "events";
import { sample, Debug, noop, defaults } from "../utils";
import { RedisOptions, getNodeKey, NodeKey, NodeRole } from "./util";
import { ClusterOptions, ClusterNodeRetryStrategy } from "./ClusterOptions";
import Redis from "../Redis";

const debug = Debug("cluster:connectionPool");

type NODE_TYPE = "all" | "master" | "slave";

type NodeRecord = {
  redis: Redis;
  endListener: () => void;
  errorListener: (error: unknown) => void;
};

export default class ConnectionPool extends EventEmitter {
  // master + slave = all
  private nodeRecords: { [key in NODE_TYPE]: { [key: string]: NodeRecord } } = {
    all: {},
    master: {},
    slave: {},
  };

  private specifiedOptions: { [key: string]: any } = {};
  private generation?: number;

  constructor(
    private redisOptions: NonNullable<ClusterOptions["redisOptions"]>,
    private clusterNodeRetryStrategy: ClusterNodeRetryStrategy = null
  ) {
    super();
  }

  getNodes(role: NodeRole = "all"): Redis[] {
    const nodeRecords = this.nodeRecords[role];
    return Object.keys(nodeRecords).map((key) => nodeRecords[key]?.redis);
  }

  getInstanceByKey(key: NodeKey): Redis {
    return this.nodeRecords.all[key]?.redis;
  }

  getSampleInstance(role: NodeRole): Redis {
    const keys = Object.keys(this.nodeRecords[role]);
    const sampleKey = sample(keys);
    return this.nodeRecords[role][sampleKey]?.redis;
  }

  /**
   * Add a master node to the pool
   * @param node
   */
  addMasterNode(node: RedisOptions) {
    const key = getNodeKey(node.options);
    const redis: Redis = this.createRedisFromOptions(
      node,
      node.options.readOnly
    );

    //Master nodes aren't read-only
    if (!node.options.readOnly) {
      const nodeRecord = this.createNodeRecord(redis, key);
      this.nodeRecords.all[key] = nodeRecord;
      this.nodeRecords.master[key] = nodeRecord;
      redis.once("end", nodeRecord.endListener);
      redis.on("error", nodeRecord.errorListener);
      return true;
    }

    return false;
  }

  /**
   * Creates a Redis connection instance from the node options
   * @param node
   * @param readOnly
   */
  createRedisFromOptions(node: RedisOptions, readOnly: boolean) {
    const redis = new Redis(
      defaults(
        {
          // By default, never try to reconnect when a node is lost,
          // instead, waiting for a `MOVED` error and fetching slots again.
          // When `clusterNodeRetryStrategy` is set, use it to allow
          // reconnection (e.g. for replica nodes that restart without
          // any slot changes).
          retryStrategy:
            typeof this.clusterNodeRetryStrategy === "function"
              ? this.clusterNodeRetryStrategy
              : null,
          // Offline queue should be enabled so that
          // we don't need to wait for the `ready` event
          // before sending commands to the node.
          enableOfflineQueue: true,
          readOnly: readOnly,
        },
        node,
        this.redisOptions,
        { lazyConnect: true }
      )
    );

    return redis;
  }

  /**
   * Find or create a connection to the node
   */
  findOrCreate(redisOptions: RedisOptions, readOnly = false): Redis {
    const key = getNodeKey(redisOptions);
    readOnly = Boolean(readOnly);

    if (this.specifiedOptions[key]) {
      Object.assign(redisOptions, this.specifiedOptions[key]);
    } else {
      this.specifiedOptions[key] = redisOptions;
    }

    let nodeRecord: NodeRecord;
    if (this.nodeRecords.all[key]) {
      nodeRecord = this.nodeRecords.all[key];
      if (nodeRecord.redis.options.readOnly !== readOnly) {
        nodeRecord.redis.options.readOnly = readOnly;
        debug("Change role of %s to %s", key, readOnly ? "slave" : "master");
        nodeRecord.redis[readOnly ? "readonly" : "readwrite"]().catch(noop);
        if (readOnly) {
          delete this.nodeRecords.master[key];
          this.nodeRecords.slave[key] = nodeRecord;
        } else {
          delete this.nodeRecords.slave[key];
          this.nodeRecords.master[key] = nodeRecord;
        }
      }
    } else {
      debug("Connecting to %s as %s", key, readOnly ? "slave" : "master");
      const redis = this.createRedisFromOptions(redisOptions, readOnly);
      nodeRecord = this.createNodeRecord(redis, key);
      this.nodeRecords.all[key] = nodeRecord;
      this.nodeRecords[readOnly ? "slave" : "master"][key] = nodeRecord;

      redis.once("end", nodeRecord.endListener);
      redis.on("error", nodeRecord.errorListener);

      this.emit("+node", redis, key);
    }

    return nodeRecord.redis;
  }

  /**
   * Reset the pool with a set of nodes.
   * The old node will be removed.
   */
  reset(nodes: RedisOptions[], generation?: number): void {
    debug("Reset with %O", nodes);
    if (generation !== undefined) {
      this.generation = generation;
    }
    const newNodes = {};
    nodes.forEach((node) => {
      const key = getNodeKey(node);

      // Don't override the existing (master) node
      // when the current one is slave.
      if (!(node.readOnly && newNodes[key])) {
        newNodes[key] = node;
      }
    });

    for (const key of Object.keys(newNodes)) {
      if (generation !== undefined && generation !== this.generation) {
        return;
      }
      const node = newNodes[key];
      this.findOrCreate(node, node.readOnly);
      if (generation !== undefined && generation !== this.generation) {
        return;
      }
    }
    for (const key of Object.keys(this.nodeRecords.all)) {
      if (generation !== undefined && generation !== this.generation) {
        return;
      }
      const nodeRecord = this.nodeRecords.all[key];
      if (!newNodes[key] && nodeRecord) {
        debug("Disconnect %s because the node does not hold any slot", key);
        nodeRecord.redis.disconnect();
        if (this.nodeRecords.all[key] === nodeRecord) {
          this.removeNode(key, nodeRecord);
          if (generation !== undefined && generation !== this.generation) {
            return;
          }
        }
      }
    }
  }

  private createNodeRecord(redis: Redis, key: NodeKey): NodeRecord {
    const nodeRecord: NodeRecord = {
      redis,
      endListener: () => {
        this.removeNode(key, nodeRecord);
      },
      errorListener: (error: unknown) => {
        this.emit("nodeError", error, key);
      },
    };
    return nodeRecord;
  }

  /**
   * Remove a node from the pool.
   */
  private removeNode(key: string, expectedRecord?: NodeRecord): void {
    const { nodeRecords } = this;
    const nodeRecord = nodeRecords.all[key];
    if (nodeRecord && (!expectedRecord || nodeRecord === expectedRecord)) {
      debug("Remove %s from the pool", key);
      nodeRecord.redis.removeListener("end", nodeRecord.endListener);
      nodeRecord.redis.removeListener("error", nodeRecord.errorListener);
      delete nodeRecords.all[key];
      delete nodeRecords.master[key];
      delete nodeRecords.slave[key];

      const shouldEmitDrain = !Object.keys(nodeRecords.all).length;
      this.emit("-node", nodeRecord.redis, key);
      if (shouldEmitDrain && !Object.keys(nodeRecords.all).length) {
        this.emit("drain", this.generation);
      }
    }
  }
}
