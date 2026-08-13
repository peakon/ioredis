import { exists, hasFlag } from "@ioredis/commands";
import { EventEmitter } from "events";
import { AbortError, RedisError } from "redis-errors";
import asCallback from "standard-as-callback";
import Command from "../Command";
import ClusterAllFailedError from "../errors/ClusterAllFailedError";
import Pipeline from "../Pipeline";
import Redis from "../Redis";
import { getConfiguredBlockingTimeout } from "../redis/blockingTimeout";
import ScanStream from "../ScanStream";
import { addTransactionSupport, Transaction } from "../transaction";
import { Callback, ScanStreamOptions, WriteableStream } from "../types";
import {
  CONNECTION_CLOSED_ERROR_MSG,
  Debug,
  defaults,
  noop,
  sample,
  shuffle,
  timeout,
  zipMap,
} from "../utils";
import applyMixin from "../utils/applyMixin";
import Commander from "../utils/Commander";
import { ClusterOptions, DEFAULT_CLUSTER_OPTIONS } from "./ClusterOptions";
import ClusterSubscriber from "./ClusterSubscriber";
import ConnectionPool from "./ConnectionPool";
import DelayQueue from "./DelayQueue";
import {
  getConnectionName,
  getUniqueHostnamesFromOptions,
  groupSrvRecords,
  NodeKey,
  nodeKeyToRedisOptions,
  NodeRole,
  normalizeNodeOptions,
  RedisOptions,
  weightSrvRecords,
} from "./util";
import Deque = require("denque");
import ClusterSubscriberGroup from "./ClusterSubscriberGroup";

const debug = Debug("cluster");

const REJECT_OVERWRITTEN_COMMANDS = new WeakSet<Command>();

type OfflineQueueItem = {
  command: Command;
  stream: WriteableStream;
  node: unknown;
};

type ConnectOperation = {
  generation: number;
  promise: Promise<void>;
  resolve: () => void;
  reject: (error: Error) => void;
  settled: boolean;
};

type RefreshOperation = {
  generation: number;
  callbacks: Callback<void>[];
  connection?: Redis;
  settled: boolean;
};

type TeardownOperation = {
  generation: number;
  promise: Promise<void>;
  resolve: () => void;
  settled: boolean;
};

type QueuedConnectOperation = {
  teardown: TeardownOperation;
  promise: Promise<void>;
};

export type ClusterNode =
  | string
  | number
  | {
      host?: string | undefined;
      port?: number | undefined;
    };

type ClusterStatus =
  | "end"
  | "close"
  | "wait"
  | "connecting"
  | "connect"
  | "ready"
  | "reconnecting"
  | "disconnecting";

/**
 * Client for the official Redis Cluster
 */
class Cluster extends Commander {
  options: ClusterOptions;
  slots: NodeKey[][] = [];
  status: ClusterStatus;

  /**
   * @ignore
   */
  _groupsIds: { [key: string]: number } = {};

  /**
   * @ignore
   */
  _groupsBySlot: number[] = Array(16384);

  /**
   * @ignore
   */
  isCluster = true;

  private startupNodes: (string | number | object)[];
  private connectionPool: ConnectionPool;
  private manuallyClosing = false;
  private retryAttempts = 0;
  private delayQueue: DelayQueue = new DelayQueue();
  private offlineQueue = new Deque<OfflineQueueItem>();
  private offlineQueueCleanupScheduled = false;
  private subscriber: ClusterSubscriber;
  private shardedSubscribers: ClusterSubscriberGroup;
  private slotsTimer: NodeJS.Timer;
  private reconnectTimeout: NodeJS.Timer;
  private activeRefresh: RefreshOperation;
  private activeConnect: ConnectOperation;
  private activeTeardown: TeardownOperation;
  private queuedConnect: QueuedConnectOperation;
  private closedGeneration = -1;
  private shardedSubscriberRetryTimers = new Set<NodeJS.Timer>();
  private _autoPipelines: Map<string, typeof Pipeline> = new Map();
  private _runningAutoPipelines: Set<string> = new Set();
  private _readyDelayedCallbacks: Callback[] = [];
  private subscriberGroupEmitter: EventEmitter | null;

  /**
   * Identifies the lifecycle that owns asynchronous cluster work. Starting a
   * connection or invalidating one during teardown advances the generation.
   */
  private connectionEpoch = 0;

  /**
   * Creates an instance of Cluster.
   */
  //TODO: Add an option that enables or disables sharded PubSub
  constructor(startupNodes: ClusterNode[], options: ClusterOptions = {}) {
    super();
    EventEmitter.call(this);

    this.startupNodes = startupNodes;
    this.options = defaults({}, options, DEFAULT_CLUSTER_OPTIONS, this.options);

    if (this.options.shardedSubscribers) {
      this.createShardedSubscriberGroup();
    }

    if (
      this.options.redisOptions &&
      this.options.redisOptions.keyPrefix &&
      !this.options.keyPrefix
    ) {
      this.options.keyPrefix = this.options.redisOptions.keyPrefix;
    }

    // validate options
    if (
      typeof this.options.scaleReads !== "function" &&
      ["all", "master", "slave"].indexOf(this.options.scaleReads) === -1
    ) {
      throw new Error(
        'Invalid option scaleReads "' +
          this.options.scaleReads +
          '". Expected "all", "master", "slave" or a custom function'
      );
    }

    this.connectionPool = new ConnectionPool(
      this.options.redisOptions ?? {},
      this.options.clusterNodeRetryStrategy
    );

    this.connectionPool.on("-node", (redis, key) => {
      this.emit("-node", redis);
    });
    this.connectionPool.on("+node", (redis) => {
      this.emit("+node", redis);
    });
    this.connectionPool.on("drain", (generation) =>
      this.handlePoolDrain(generation)
    );
    this.connectionPool.on("nodeError", (error, key) => {
      this.emit("node error", error, key);
    });

    this.subscriber = new ClusterSubscriber(this.connectionPool, this);

    if (this.options.scripts) {
      Object.entries(this.options.scripts).forEach(([name, definition]) => {
        this.defineCommand(name, definition);
      });
    }

    if (this.options.lazyConnect) {
      this.setStatus("wait");
    } else {
      this.connect().catch((err) => {
        debug("connecting failed: %s", err);
      });
    }
  }

  /**
   * Connect to a cluster.
   *
   * Concurrent calls share the active connection attempt. Calling this method
   * after the cluster is ready rejects with an already-connected error.
   */
  connect(): Promise<void> {
    if (this.activeTeardown && !this.activeTeardown.settled) {
      // disconnect() tears down synchronously. This branch coordinates with
      // asynchronous quit() and connect() calls made re-entrantly by teardown
      // listeners.
      const teardown = this.activeTeardown;
      if (!this.queuedConnect || this.queuedConnect.teardown !== teardown) {
        const queuedConnect: QueuedConnectOperation = {
          teardown,
          promise: undefined,
        };
        queuedConnect.promise = teardown.promise.then(() => {
          if (this.queuedConnect === queuedConnect) {
            this.queuedConnect = undefined;
          }
          if (
            this.connectionEpoch !== teardown.generation ||
            (this.activeTeardown &&
              this.activeTeardown !== teardown &&
              !this.activeTeardown.settled)
          ) {
            throw new RedisError(CONNECTION_CLOSED_ERROR_MSG);
          }
          return this.connect();
        });
        this.queuedConnect = queuedConnect;
      }
      return this.queuedConnect.promise;
    }

    if (
      this.activeConnect &&
      this.activeConnect.generation === this.connectionEpoch &&
      (this.status === "connecting" || this.status === "connect")
    ) {
      return this.activeConnect.promise;
    }

    if (this.status === "ready") {
      return Promise.reject(new Error("Redis is already connecting/connected"));
    }

    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }

    const generation = ++this.connectionEpoch;
    this.manuallyClosing = false;
    const canceledConnection = this.cancelActiveConnection(
      new RedisError("Connection is discarded because a new connection is made")
    );
    this.cancelRefresh(new RedisError(CONNECTION_CLOSED_ERROR_MSG));
    if (canceledConnection) {
      this.invokeReadyDelayedCallbacks(
        new RedisError(CONNECTION_CLOSED_ERROR_MSG)
      );
    }

    let resolve: () => void;
    let reject: (error: Error) => void;
    const promise = new Promise<void>((resolvePromise, rejectPromise) => {
      resolve = resolvePromise;
      reject = rejectPromise;
    });
    const operation: ConnectOperation = {
      generation,
      promise,
      resolve,
      reject,
      settled: false,
    };
    this.activeConnect = operation;
    this.setStatus("connecting", generation);
    this.startConnection(operation);

    return promise;
  }

  /**
   * Disconnect from every node in the cluster.
   */
  disconnect(reconnect = false) {
    if (!reconnect && this.status === "end") {
      return;
    }

    if (
      reconnect &&
      (this.status === "reconnecting" || this.status === "disconnecting")
    ) {
      return;
    }

    this.teardown(reconnect);
  }

  private startConnection(operation: ConnectOperation): void {
    const { generation } = operation;

    this.resolveStartupNodeHostnames()
      .then((nodes) => {
        if (!this.isConnectionOwner(operation)) {
          return;
        }

        this.connectionPool.reset(nodes, generation);
        if (!this.isConnectionOwner(operation)) {
          return;
        }

        if (this.options.shardedSubscribers) {
          this.shardedSubscribers
            .reset(this.slots, this.connectionPool.getNodes("all"))
            .catch((err) => {
              if (this.connectionEpoch === generation) {
                debug("Error while starting subscribers: %s", err);
              }
            });
        }

        this.refreshSlotsCacheForGeneration((err) => {
          if (!this.isConnectionOwner(operation)) {
            return;
          }

          if (err) {
            if (err.message === ClusterAllFailedError.defaultMessage) {
              Redis.prototype.silentEmit.call(this, "error", err);
              if (!this.isConnectionOwner(operation)) {
                return;
              }
              this.failConnection(
                operation,
                new Error("None of startup nodes is available"),
                err
              );
              if (generation === this.connectionEpoch) {
                this.connectionPool.reset([], generation);
              }
            } else {
              this.failConnection(operation, err, err);
            }
            return;
          }

          this.invokeReadyDelayedCallbacks(undefined);
          this.setStatus("connect", generation);
          if (this.options.enableReadyCheck) {
            this.readyCheck((readyError, fail) => {
              if (!this.isConnectionOwner(operation)) {
                return;
              }
              if (readyError || fail) {
                debug(
                  "Ready check failed (%s). Reconnecting...",
                  readyError || fail
                );
                this.forceReconnect(
                  generation,
                  readyError || new Error(`Cluster state is ${fail}`)
                );
              } else {
                this.completeConnection(operation);
              }
            });
          } else {
            this.completeConnection(operation);
          }
        }, generation);

        if (!this.isConnectionOwner(operation)) {
          return;
        }

        this.subscriber.start();
        if (this.options.shardedSubscribers) {
          this.shardedSubscribers.start().catch((err) => {
            if (this.connectionEpoch === generation) {
              debug("Error while starting subscribers: %s", err);
            }
          });
        }
      })
      .catch((err) => {
        if (this.isConnectionOwner(operation)) {
          this.failConnection(operation, err, err);
        }
      });
  }

  private completeConnection(operation: ConnectOperation): void {
    if (!this.isConnectionOwner(operation)) {
      return;
    }

    this.setStatus("ready", operation.generation);
    this.retryAttempts = 0;
    this.executeOfflineCommands();
    this.resetNodesRefreshInterval(operation.generation);
    this.settleConnection(operation, undefined);
  }

  private failConnection(
    operation: ConnectOperation,
    error: Error,
    closeReason?: Error
  ): void {
    if (!this.isConnectionOwner(operation)) {
      return;
    }

    this.settleConnection(operation, error);
    this.invokeReadyDelayedCallbacks(error);
    this.finishClose(operation.generation, closeReason);
  }

  private settleConnection(operation: ConnectOperation, error?: Error): void {
    if (operation.settled) {
      return;
    }

    operation.settled = true;
    if (this.activeConnect === operation) {
      this.activeConnect = undefined;
    }
    if (error) {
      operation.reject(error);
    } else {
      operation.resolve();
    }
  }

  private isConnectionOwner(operation: ConnectOperation): boolean {
    return (
      !operation.settled &&
      this.activeConnect === operation &&
      operation.generation === this.connectionEpoch
    );
  }

  private cancelActiveConnection(error: Error): boolean {
    if (this.activeConnect) {
      this.settleConnection(this.activeConnect, error);
      return true;
    }
    return false;
  }

  private cancelLifecycleWork(error: Error): void {
    this.clearNodesRefreshInterval();
    this.clearShardedSubscriberRetryTimers();
    this.delayQueue.flush();
    this.cancelActiveConnection(error);
    this.cancelRefresh(error, true);
    this.invokeReadyDelayedCallbacks(error);

    this.subscriber.stop();
    if (this.options.shardedSubscribers) {
      this.shardedSubscribers.stop();
    }
  }

  private teardown(
    reconnect: boolean,
    reason?: Error,
    connectionError = new RedisError(CONNECTION_CLOSED_ERROR_MSG)
  ): void {
    const previousStatus = this.status;
    const generation = ++this.connectionEpoch;
    const operation = this.beginTeardown(generation);
    this.manuallyClosing = !reconnect;
    this.setStatus("disconnecting", generation);

    try {
      if (this.reconnectTimeout) {
        clearTimeout(this.reconnectTimeout);
        this.reconnectTimeout = null;
        debug("Canceled reconnecting attempts");
      }

      this.cancelLifecycleWork(connectionError);

      this.connectionPool.reset([], generation);
      if (
        !reconnect &&
        (previousStatus === "close" ||
          previousStatus === "reconnecting" ||
          previousStatus === "end")
      ) {
        this.closedGeneration = generation;
        this.handleCloseEvent(reason, generation);
      } else {
        this.finishClose(generation, reason);
      }
    } finally {
      this.settleTeardown(operation);
    }
  }

  private beginTeardown(generation: number): TeardownOperation {
    let resolve: () => void;
    const promise = new Promise<void>((resolvePromise) => {
      resolve = resolvePromise;
    });
    const operation = { generation, promise, resolve, settled: false };
    this.activeTeardown = operation;
    return operation;
  }

  private settleTeardown(operation: TeardownOperation): void {
    if (operation.settled) {
      return;
    }
    operation.settled = true;
    if (this.activeTeardown === operation) {
      this.activeTeardown = undefined;
    }
    operation.resolve();
  }

  private forceReconnect(generation: number, reason?: Error): void {
    if (generation !== this.connectionEpoch) {
      return;
    }

    this.teardown(
      true,
      reason,
      new Error("None of startup nodes is available")
    );
  }

  private handlePoolDrain(generation?: number): void {
    if (generation !== undefined && generation !== this.connectionEpoch) {
      return;
    }
    generation ??= this.connectionEpoch;
    if (
      this.activeTeardown &&
      !this.activeTeardown.settled &&
      this.activeTeardown.generation === generation
    ) {
      return;
    }
    if (
      this.status === "close" ||
      this.status === "end" ||
      this.status === "reconnecting"
    ) {
      return;
    }

    const connectionError = new Error("None of startup nodes is available");
    if (
      this.activeConnect &&
      (this.status === "connecting" || this.status === "connect")
    ) {
      this.settleConnection(this.activeConnect, connectionError);
      this.invokeReadyDelayedCallbacks(connectionError);
    }
    if (generation !== this.connectionEpoch) {
      return;
    }

    this.clearShardedSubscriberRetryTimers();
    this.cancelRefresh(new RedisError(CONNECTION_CLOSED_ERROR_MSG));
    this.clearNodesRefreshInterval();
    if (generation !== this.connectionEpoch) {
      return;
    }

    this.delayQueue.flush();
    if (generation !== this.connectionEpoch) {
      return;
    }

    this.subscriber.stop();
    if (this.options.shardedSubscribers) {
      this.shardedSubscribers.stop();
    }
    if (generation !== this.connectionEpoch) {
      return;
    }

    this.finishClose(generation);
  }

  private clearShardedSubscriberRetryTimers(): void {
    for (const timer of this.shardedSubscriberRetryTimers) {
      clearTimeout(timer);
    }
    this.shardedSubscriberRetryTimers.clear();
  }

  /**
   * Quit the cluster gracefully.
   */
  quit(callback?: Callback<"OK">): Promise<"OK"> {
    const generation = ++this.connectionEpoch;
    const operation = this.beginTeardown(generation);
    this.manuallyClosing = true;
    this.setStatus("disconnecting", generation);

    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }
    const error = new RedisError(CONNECTION_CLOSED_ERROR_MSG);
    this.cancelLifecycleWork(error);

    const nodes = this.nodes();
    if (nodes.length === 0) {
      const result = Promise.resolve<"OK">("OK");

      // use setImmediate to make sure "close" event
      // being emitted after quit() is returned
      setImmediate(() => {
        this.finishClose(generation);
        this.settleTeardown(operation);
      });

      return asCallback(result, callback);
    }
    const result = Promise.all(
      nodes.map((node) =>
        node.quit().catch((err) => {
          // Ignore the error caused by disconnecting since
          // we're disconnecting...
          if (err.message === CONNECTION_CLOSED_ERROR_MSG) {
            return "OK";
          }
          throw err;
        })
      )
    )
      .then(() => "OK" as const)
      .finally(() => {
        try {
          if (generation === this.connectionEpoch) {
            this.connectionPool.reset([], generation);
            this.finishClose(generation);
          }
        } finally {
          this.settleTeardown(operation);
        }
      });
    return asCallback(result, callback);
  }

  /**
   * Create a new instance with the same startup nodes and options as the current one.
   *
   * @example
   * ```js
   * var cluster = new Redis.Cluster([{ host: "127.0.0.1", port: "30001" }]);
   * var anotherCluster = cluster.duplicate();
   * ```
   */
  duplicate(overrideStartupNodes = [], overrideOptions = {}) {
    const startupNodes =
      overrideStartupNodes.length > 0
        ? overrideStartupNodes
        : this.startupNodes.slice(0);
    const options = Object.assign({}, this.options, overrideOptions);
    return new Cluster(startupNodes, options);
  }

  /**
   * Get nodes with the specified role
   */
  nodes(role: NodeRole = "all"): Redis[] {
    if (role !== "all" && role !== "master" && role !== "slave") {
      throw new Error(
        'Invalid role "' + role + '". Expected "all", "master" or "slave"'
      );
    }
    return this.connectionPool.getNodes(role);
  }

  /**
   * This is needed in order not to install a listener for each auto pipeline
   *
   * @ignore
   */
  delayUntilReady(callback: Callback) {
    this._readyDelayedCallbacks.push(callback);
  }

  /**
   * Get the number of commands queued in automatic pipelines.
   *
   * This is not available (and returns 0) until the cluster is connected and slots information have been received.
   */
  get autoPipelineQueueSize(): number {
    let queued = 0;

    for (const pipeline of this._autoPipelines.values()) {
      queued += pipeline.length;
    }

    return queued;
  }

  /**
   * Refresh the slot cache
   *
   * @ignore
   */
  refreshSlotsCache(callback?: Callback<void>): void {
    this.refreshSlotsCacheForGeneration(callback, this.connectionEpoch);
  }

  private refreshSlotsCacheForGeneration(
    callback: Callback<void> | undefined,
    generation: number
  ): void {
    if (!this.isRefreshAllowed(generation)) {
      if (callback) {
        process.nextTick(callback, new RedisError(CONNECTION_CLOSED_ERROR_MSG));
      }
      return;
    }

    if (
      this.activeRefresh &&
      !this.activeRefresh.settled &&
      this.activeRefresh.generation === generation
    ) {
      if (callback) {
        this.activeRefresh.callbacks.push(callback);
      }
      return;
    }

    const operation: RefreshOperation = {
      generation,
      callbacks: callback ? [callback] : [],
      settled: false,
    };
    this.activeRefresh = operation;
    const nodes = shuffle(this.connectionPool.getNodes());
    let lastNodeError: Error = null;

    const tryNode = (index: number) => {
      if (!this.isRefreshOwner(operation)) {
        return;
      }
      if (index === nodes.length) {
        this.settleRefresh(
          operation,
          new ClusterAllFailedError(
            ClusterAllFailedError.defaultMessage,
            lastNodeError
          )
        );
        return;
      }

      const node = nodes[index];
      const key = `${node.options.host}:${node.options.port}`;
      debug("getting slot cache from %s", key);
      this.getInfoFromNode(node, operation, (err) => {
        if (!this.isRefreshOwner(operation)) {
          return;
        }
        if (err) {
          this.emit("node error", err, key);
          lastNodeError = err;
          tryNode(index + 1);
        } else {
          this.emit("refresh");
          this.settleRefresh(operation);
        }
      });
    };

    tryNode(0);
  }

  private isRefreshOwner(operation: RefreshOperation): boolean {
    return (
      !operation.settled &&
      this.activeRefresh === operation &&
      this.isRefreshAllowed(operation.generation)
    );
  }

  private isRefreshAllowed(generation: number): boolean {
    // Slot state belongs to an active connection lifecycle and must not be
    // refreshed while waiting, reconnecting, tearing down, or closed.
    return (
      generation === this.connectionEpoch &&
      (this.status === "connecting" ||
        this.status === "connect" ||
        this.status === "ready")
    );
  }

  private settleRefresh(
    operation: RefreshOperation,
    error?: Error,
    deferCallbacks = false
  ): void {
    if (operation.settled) {
      return;
    }

    operation.settled = true;
    if (this.activeRefresh === operation) {
      this.activeRefresh = undefined;
    }
    const callbacks = operation.callbacks;
    operation.callbacks = [];
    if (deferCallbacks) {
      const scheduleCallbacks = () => {
        for (const callback of callbacks) {
          process.nextTick(callback, error);
        }
      };
      if (this.activeTeardown && !this.activeTeardown.settled) {
        this.activeTeardown.promise.then(scheduleCallbacks);
      } else {
        scheduleCallbacks();
      }
      return;
    }

    for (const callback of callbacks) {
      callback(error);
    }
  }

  private cancelRefresh(error: Error, deferCallbacks = false): void {
    const operation = this.activeRefresh;
    if (!operation || operation.settled) {
      return;
    }

    operation.connection?.disconnect();
    operation.connection = undefined;
    this.settleRefresh(operation, error, deferCallbacks);
  }

  /**
   * @ignore
   */
  sendCommand(command: Command, stream?: WriteableStream, node?: any): unknown {
    if (this.status === "wait") {
      this.connect().catch(noop);
    }
    if (this.status === "end") {
      command.reject(new Error(CONNECTION_CLOSED_ERROR_MSG));
      return command.promise;
    }
    let to = this.options.scaleReads;
    if (to !== "master") {
      const isCommandReadOnly =
        command.isReadOnly ||
        (exists(command.name) && hasFlag(command.name, "readonly"));
      if (!isCommandReadOnly) {
        to = "master";
      }
    }

    let targetSlot = node ? node.slot : command.getSlot();
    const ttl = {};
    const _this = this;
    if (!node && !REJECT_OVERWRITTEN_COMMANDS.has(command)) {
      REJECT_OVERWRITTEN_COMMANDS.add(command);

      const reject = command.reject;
      command.reject = function (err) {
        const partialTry = tryConnection.bind(null, true);
        _this.handleError(err, ttl, {
          moved: function (slot, key) {
            debug("command %s is moved to %s", command.name, key);
            targetSlot = Number(slot);
            if (_this.slots[slot]) {
              _this.slots[slot][0] = key;
            } else {
              _this.slots[slot] = [key];
            }
            _this._groupsBySlot[slot] =
              _this._groupsIds[_this.slots[slot].join(";")];
            _this.connectionPool.findOrCreate(_this.natMapper(key));
            tryConnection();
            debug("refreshing slot caches... (triggered by MOVED error)");
            _this.refreshSlotsCache();
          },
          ask: function (slot, key) {
            debug("command %s is required to ask %s:%s", command.name, key);
            const mapped = _this.natMapper(key);
            _this.connectionPool.findOrCreate(mapped);
            tryConnection(false, `${mapped.host}:${mapped.port}`);
          },
          tryagain: partialTry,
          clusterDown: partialTry,
          connectionClosed: partialTry,
          maxRedirections: function (redirectionError) {
            reject.call(command, redirectionError);
          },
          defaults: function () {
            reject.call(command, err);
          },
        });
      };
    }
    tryConnection();

    function tryConnection(random?: boolean, asking?: string) {
      if (command.isResolved) {
        return;
      }
      if (_this.status === "end") {
        command.reject(new AbortError("Cluster is ended."));
        return;
      }
      let redis;
      if (_this.status === "ready" || command.name === "cluster") {
        if (node && node.redis) {
          redis = node.redis;
        } else if (
          Command.checkFlag("ENTER_SUBSCRIBER_MODE", command.name) ||
          Command.checkFlag("EXIT_SUBSCRIBER_MODE", command.name)
        ) {
          if (
            _this.options.shardedSubscribers &&
            (command.name == "ssubscribe" || command.name == "sunsubscribe")
          ) {
            const sub =
              _this.shardedSubscribers.getResponsibleSubscriber(targetSlot);

            if (!sub) {
              command.reject(
                new AbortError(`No sharded subscriber for slot: ${targetSlot}`)
              );
              return;
            }

            let status = -1;

            if (command.name == "ssubscribe") {
              status = _this.shardedSubscribers.addChannels(command.getKeys());
            }

            if (command.name == "sunsubscribe") {
              status = _this.shardedSubscribers.removeChannels(
                command.getKeys()
              );
            }

            if (status !== -1) {
              redis = sub.getInstance();
            } else {
              command.reject(
                new AbortError(
                  "Possible CROSSSLOT error: All channels must hash to the same slot"
                )
              );
            }
          } else {
            redis = _this.subscriber.getInstance();
          }

          if (!redis) {
            command.reject(new AbortError("No subscriber for the cluster"));
            return;
          }
        } else {
          if (!random) {
            if (typeof targetSlot === "number" && _this.slots[targetSlot]) {
              const nodeKeys = _this.slots[targetSlot];
              if (typeof to === "function") {
                const nodes = nodeKeys.map(function (key) {
                  return _this.connectionPool.getInstanceByKey(key);
                });
                redis = to(nodes, command);
                if (Array.isArray(redis)) {
                  redis = sample(redis);
                }
                if (!redis) {
                  redis = nodes[0];
                }
              } else {
                let key: string;
                if (to === "all") {
                  key = sample(nodeKeys);
                } else if (to === "slave" && nodeKeys.length > 1) {
                  key = sample(nodeKeys, 1);
                } else {
                  key = nodeKeys[0];
                }
                redis = _this.connectionPool.getInstanceByKey(key);
              }
            }
            if (asking) {
              redis = _this.connectionPool.getInstanceByKey(asking);
              redis.asking();
            }
          }
          if (!redis) {
            redis =
              (typeof to === "function"
                ? null
                : _this.connectionPool.getSampleInstance(to)) ||
              _this.connectionPool.getSampleInstance("all");
          }
          if (
            redis &&
            !_this.options.enableOfflineQueue &&
            redis.status !== "ready" &&
            redis.status !== "wait"
          ) {
            command.reject(new Error(CONNECTION_CLOSED_ERROR_MSG));
            return;
          }
        }
        if (node && !node.redis) {
          node.redis = redis;
        }
      }
      if (!redis && _this.options.enableOfflineQueue) {
        const offlineItem = {
          command: command,
          stream: stream,
          node: node,
        };
        _this.offlineQueue.push(offlineItem);
        const blockingTimeout = Command.checkFlag(
          "BLOCKING_COMMANDS",
          command.name
        )
          ? getConfiguredBlockingTimeout(_this.options.redisOptions)
          : undefined;
        if (blockingTimeout !== undefined) {
          command.setBlockingTimeout(blockingTimeout);
          command.promise.then(() => _this.scheduleOfflineQueueCleanup(), noop);
        }
        return;
      }

      if (!redis) {
        command.reject(
          new Error(
            "Cluster isn't ready and enableOfflineQueue options is false"
          )
        );
        return;
      }

      redis.sendCommand(command, stream);
    }
    return command.promise;
  }

  sscanStream(key: string, options?: ScanStreamOptions) {
    return this.createScanStream("sscan", { key, options });
  }

  sscanBufferStream(key: string, options?: ScanStreamOptions) {
    return this.createScanStream("sscanBuffer", { key, options });
  }

  hscanStream(key: string, options?: ScanStreamOptions) {
    return this.createScanStream("hscan", { key, options });
  }

  hscanBufferStream(key: string, options?: ScanStreamOptions) {
    return this.createScanStream("hscanBuffer", { key, options });
  }

  zscanStream(key: string, options?: ScanStreamOptions) {
    return this.createScanStream("zscan", { key, options });
  }

  zscanBufferStream(key: string, options?: ScanStreamOptions) {
    return this.createScanStream("zscanBuffer", { key, options });
  }

  /**
   * @ignore
   */
  handleError(error: Error, ttl: { value?: any }, handlers) {
    if (typeof ttl.value === "undefined") {
      ttl.value = this.options.maxRedirections;
    } else {
      ttl.value -= 1;
    }
    if (ttl.value <= 0) {
      handlers.maxRedirections(
        new Error("Too many Cluster redirections. Last error: " + error)
      );
      return;
    }
    const errv = error.message.split(" ");
    if (errv[0] === "MOVED") {
      const timeout = this.options.retryDelayOnMoved;
      if (timeout && typeof timeout === "number") {
        this.delayQueue.push(
          "moved",
          handlers.moved.bind(null, errv[1], errv[2]),
          { timeout }
        );
      } else {
        handlers.moved(errv[1], errv[2]);
      }
    } else if (errv[0] === "ASK") {
      handlers.ask(errv[1], errv[2]);
    } else if (errv[0] === "TRYAGAIN") {
      this.delayQueue.push("tryagain", handlers.tryagain, {
        timeout: this.options.retryDelayOnTryAgain,
      });
    } else if (
      errv[0] === "CLUSTERDOWN" &&
      this.options.retryDelayOnClusterDown > 0
    ) {
      this.delayQueue.push("clusterdown", handlers.connectionClosed, {
        timeout: this.options.retryDelayOnClusterDown,
        callback: this.refreshSlotsCache.bind(this),
      });
    } else if (
      error.message === CONNECTION_CLOSED_ERROR_MSG &&
      this.options.retryDelayOnFailover > 0 &&
      this.status === "ready"
    ) {
      this.delayQueue.push("failover", handlers.connectionClosed, {
        timeout: this.options.retryDelayOnFailover,
        callback: this.refreshSlotsCache.bind(this),
      });
    } else {
      handlers.defaults();
    }
  }

  private resetOfflineQueue() {
    this.offlineQueue = new Deque();
  }

  private scheduleOfflineQueueCleanup(): void {
    if (this.offlineQueueCleanupScheduled || !this.offlineQueue.length) {
      return;
    }

    this.offlineQueueCleanupScheduled = true;
    setImmediate(() => {
      this.offlineQueueCleanupScheduled = false;
      if (!this.offlineQueue.length) {
        return;
      }
      const offlineQueue = this.offlineQueue;
      this.resetOfflineQueue();
      let item: OfflineQueueItem;
      while ((item = offlineQueue.shift())) {
        if (item.command.isResolved) {
          this.discardOfflineQueueItem(item);
        } else {
          this.offlineQueue.push(item);
        }
      }
    });
  }

  private discardOfflineQueueItem(item: OfflineQueueItem): void {
    if (item.stream && "isPipeline" in item.stream && item.stream.isPipeline) {
      item.stream.writeSkipped?.();
    }
  }

  private clearNodesRefreshInterval() {
    if (this.slotsTimer) {
      clearTimeout(this.slotsTimer);
      this.slotsTimer = null;
    }
  }

  private resetNodesRefreshInterval(generation = this.connectionEpoch) {
    if (this.slotsTimer || !this.options.slotsRefreshInterval) {
      return;
    }
    const nextRound = () => {
      if (generation !== this.connectionEpoch || this.status !== "ready") {
        return;
      }
      this.slotsTimer = setTimeout(() => {
        this.slotsTimer = null;
        if (generation !== this.connectionEpoch || this.status !== "ready") {
          return;
        }
        debug(
          'refreshing slot caches... (triggered by "slotsRefreshInterval" option)'
        );
        this.refreshSlotsCacheForGeneration(() => {
          nextRound();
        }, generation);
      }, this.options.slotsRefreshInterval);
    };

    nextRound();
  }

  /**
   * Change cluster instance's status
   */
  private setStatus(
    status: ClusterStatus,
    generation = this.connectionEpoch
  ): void {
    if (generation !== this.connectionEpoch) {
      return;
    }
    debug("status: %s -> %s", this.status || "[empty]", status);
    this.status = status;
    process.nextTick(() => {
      this.emit(status);
    });
  }

  private finishClose(generation: number, reason?: Error): void {
    if (
      generation !== this.connectionEpoch ||
      this.closedGeneration === generation
    ) {
      return;
    }

    this.closedGeneration = generation;
    this.setStatus("close", generation);
    this.handleCloseEvent(reason, generation);
  }

  /**
   * Called when closed to check whether a reconnection should be made
   */
  private handleCloseEvent(
    reason?: Error,
    generation = this.connectionEpoch
  ): void {
    if (generation !== this.connectionEpoch) {
      return;
    }
    if (reason) {
      debug("closed because %s", reason);
    }

    let retryDelay: unknown;
    if (
      !this.manuallyClosing &&
      typeof this.options.clusterRetryStrategy === "function"
    ) {
      retryDelay = this.options.clusterRetryStrategy.call(
        this,
        ++this.retryAttempts,
        reason
      );
    }
    if (generation !== this.connectionEpoch) {
      return;
    }
    if (typeof retryDelay === "number") {
      this.setStatus("reconnecting", generation);
      const reconnectTimeout = setTimeout(() => {
        if (this.reconnectTimeout === reconnectTimeout) {
          this.reconnectTimeout = null;
        }
        if (
          generation !== this.connectionEpoch ||
          this.status !== "reconnecting" ||
          this.manuallyClosing
        ) {
          return;
        }
        debug("Cluster is disconnected. Retrying after %dms", retryDelay);
        this.connect().catch(function (err) {
          debug("Got error %s when reconnecting. Ignoring...", err);
        });
      }, retryDelay);
      this.reconnectTimeout = reconnectTimeout;
    } else {
      this.clearShardedSubscriberRetryTimers();
      if (this.options.shardedSubscribers) {
        this.shardedSubscribers.stop();
      }
      this.setStatus("end", generation);
      this.flushQueue(new Error("None of startup nodes is available"));
    }
  }

  /**
   * Flush offline queue with error.
   */
  private flushQueue(error: Error) {
    let item: OfflineQueueItem;
    while ((item = this.offlineQueue.shift())) {
      item.command.reject(error);
    }
  }

  private executeOfflineCommands() {
    if (this.offlineQueue.length) {
      debug("send %d commands in offline queue", this.offlineQueue.length);
      const offlineQueue = this.offlineQueue;
      this.resetOfflineQueue();
      let item: OfflineQueueItem;
      while ((item = offlineQueue.shift())) {
        if (item.command.isResolved) {
          this.discardOfflineQueueItem(item);
        } else {
          this.sendCommand(item.command, item.stream, item.node);
        }
      }
    }
  }

  private natMapper(nodeKey: NodeKey | RedisOptions): RedisOptions {
    const key =
      typeof nodeKey === "string" ? nodeKey : `${nodeKey.host}:${nodeKey.port}`;

    let mapped = null;
    if (this.options.natMap && typeof this.options.natMap === "function") {
      mapped = this.options.natMap(key);
    } else if (this.options.natMap && typeof this.options.natMap === "object") {
      mapped = this.options.natMap[key];
    }

    if (mapped) {
      debug("NAT mapping %s -> %O", key, mapped);
      return Object.assign({}, mapped);
    }

    return typeof nodeKey === "string"
      ? nodeKeyToRedisOptions(nodeKey)
      : nodeKey;
  }

  private getInfoFromNode(
    redis: Redis,
    operation: RefreshOperation,
    callback: Callback<void>
  ) {
    if (!redis) {
      return callback(new Error("Node is disconnected"));
    }

    // Use a duplication of the connection to avoid
    // timeouts when the connection is in the blocking
    // mode (e.g. waiting for BLPOP).
    const duplicatedConnection = redis.duplicate({
      enableOfflineQueue: true,
      enableReadyCheck: false,
      retryStrategy: null,
      connectionName: getConnectionName(
        "refresher",
        this.options.redisOptions && this.options.redisOptions.connectionName
      ),
    });

    // Ignore error events since we will handle
    // exceptions for the CLUSTER SLOTS command.
    duplicatedConnection.on("error", noop);
    operation.connection = duplicatedConnection;

    duplicatedConnection.cluster(
      "SLOTS",
      timeout((err: Error, result) => {
        operation.connection = undefined;
        duplicatedConnection.disconnect();
        if (err) {
          debug("error encountered running CLUSTER.SLOTS: %s", err);
          return callback(err);
        }
        if (!this.isRefreshOwner(operation)) {
          debug(
            "ignore CLUSTER.SLOTS results (count: %d) from lifecycle generation %d",
            result.length,
            operation.generation
          );
          return callback(new RedisError(CONNECTION_CLOSED_ERROR_MSG));
        }
        const slots: NodeKey[][] = this.slots.slice();
        const nodes: RedisOptions[] = [];

        debug("cluster slots result count: %d", result.length);

        for (let i = 0; i < result.length; ++i) {
          const items = result[i];
          const slotRangeStart = items[0];
          const slotRangeEnd = items[1];

          const keys = [];
          for (let j = 2; j < items.length; j++) {
            if (!items[j][0]) {
              continue;
            }
            const node = this.natMapper({
              host: items[j][0],
              port: items[j][1],
            });
            node.readOnly = j !== 2;
            nodes.push(node);
            keys.push(node.host + ":" + node.port);
          }

          debug(
            "cluster slots result [%d]: slots %d~%d served by %s",
            i,
            slotRangeStart,
            slotRangeEnd,
            keys
          );

          for (let slot = slotRangeStart; slot <= slotRangeEnd; slot++) {
            slots[slot] = keys;
          }
        }

        // Assign to each node keys a numeric value to make autopipeline comparison faster.
        const groupsIds = Object.create(null);
        const groupsBySlot: number[] = Array(16384);
        let j = 0;
        for (let i = 0; i < 16384; i++) {
          const target = (slots[i] || []).join(";");

          if (!target.length) {
            groupsBySlot[i] = undefined;
            continue;
          }

          if (!groupsIds[target]) {
            groupsIds[target] = ++j;
          }

          groupsBySlot[i] = groupsIds[target];
        }

        if (!this.isRefreshOwner(operation)) {
          return callback(new RedisError(CONNECTION_CLOSED_ERROR_MSG));
        }

        this.slots = slots;
        this._groupsIds = groupsIds;
        this._groupsBySlot = groupsBySlot;
        this.connectionPool.reset(nodes, operation.generation);

        if (this.options.shardedSubscribers && this.isRefreshOwner(operation)) {
          this.shardedSubscribers
            .reset(this.slots, this.connectionPool.getNodes("all"))
            .catch((err) => {
              if (this.connectionEpoch === operation.generation) {
                debug("Error while starting subscribers: %s", err);
              }
            });
        }
        callback();
      }, this.options.slotsRefreshTimeout)
    );
  }

  private invokeReadyDelayedCallbacks(err?: Error) {
    for (const c of this._readyDelayedCallbacks) {
      process.nextTick(c, err);
    }

    this._readyDelayedCallbacks = [];
  }

  /**
   * Check whether Cluster is able to process commands
   */
  private readyCheck(callback: Callback<void | "fail">): void {
    this.cluster("INFO", (err, res) => {
      if (err) {
        return callback(err);
      }
      if (typeof res !== "string") {
        return callback();
      }

      let state: string;
      const lines = res.split("\r\n");
      for (let i = 0; i < lines.length; ++i) {
        const parts = lines[i].split(":");
        if (parts[0] === "cluster_state") {
          state = parts[1];
          break;
        }
      }

      if (state === "fail") {
        debug("cluster state not ok (%s)", state);
        callback(null, state);
      } else {
        callback();
      }
    });
  }

  private resolveSrv(hostname: string): Promise<RedisOptions> {
    return new Promise((resolve, reject) => {
      this.options.resolveSrv(hostname, (err, records) => {
        if (err) {
          return reject(err);
        }

        const self = this,
          groupedRecords = groupSrvRecords(records),
          sortedKeys = Object.keys(groupedRecords).sort(
            (a, b) => parseInt(a) - parseInt(b)
          );

        function tryFirstOne(err?) {
          if (!sortedKeys.length) {
            return reject(err);
          }

          const key = sortedKeys[0],
            group = groupedRecords[key],
            record = weightSrvRecords(group);

          if (!group.records.length) {
            sortedKeys.shift();
          }

          self.dnsLookup(record.name).then(
            (host) =>
              resolve({
                host,
                port: record.port,
              }),
            tryFirstOne
          );
        }

        tryFirstOne();
      });
    });
  }

  private dnsLookup(hostname: string): Promise<string> {
    return new Promise((resolve, reject) => {
      this.options.dnsLookup(hostname, (err, address) => {
        if (err) {
          debug(
            "failed to resolve hostname %s to IP: %s",
            hostname,
            err.message
          );
          reject(err);
        } else {
          debug("resolved hostname %s to IP %s", hostname, address);
          resolve(address);
        }
      });
    });
  }

  /**
   * Normalize startup nodes, and resolving hostnames to IPs.
   *
   * This process happens every time when #connect() is called since
   * #startupNodes and DNS records may chanage.
   */
  private async resolveStartupNodeHostnames(): Promise<RedisOptions[]> {
    if (!Array.isArray(this.startupNodes) || this.startupNodes.length === 0) {
      throw new Error("`startupNodes` should contain at least one node.");
    }
    const startupNodes = normalizeNodeOptions(this.startupNodes);

    const hostnames = getUniqueHostnamesFromOptions(startupNodes);
    if (hostnames.length === 0) {
      return startupNodes;
    }

    const configs = await Promise.all(
      hostnames.map(
        (this.options.useSRVRecords ? this.resolveSrv : this.dnsLookup).bind(
          this
        )
      )
    );
    const hostnameToConfig = zipMap(hostnames, configs);

    return startupNodes.map((node) => {
      const config = hostnameToConfig.get(node.host);
      if (!config) {
        return node;
      }
      if (this.options.useSRVRecords) {
        return Object.assign({}, node, config);
      }
      return Object.assign({}, node, { host: config });
    });
  }

  private createScanStream(
    command: string,
    { key, options = {} }: { key?: string; options?: ScanStreamOptions }
  ) {
    return new ScanStream({
      objectMode: true,
      key: key,
      redis: this,
      command: command,
      ...options,
    });
  }

  private createShardedSubscriberGroup() {
    this.subscriberGroupEmitter = new EventEmitter();

    this.shardedSubscribers = new ClusterSubscriberGroup(
      this.subscriberGroupEmitter,
      this.options
    );

    const refreshShardedSubscriberSlots = (generation: number) => {
      if (
        generation !== this.connectionEpoch ||
        this.status === "disconnecting" ||
        this.status === "close" ||
        this.status === "end"
      ) {
        return;
      }

      this.refreshSlotsCacheForGeneration((err?: Error) => {
        if (
          generation === this.connectionEpoch &&
          err instanceof ClusterAllFailedError
        ) {
          this.forceReconnect(generation, err);
        }
      }, generation);
    };

    this.subscriberGroupEmitter.on("-node", (redis, nodeKey) => {
      const generation = this.connectionEpoch;
      this.emit("-node", redis, nodeKey);
      refreshShardedSubscriberSlots(generation);
    });

    this.subscriberGroupEmitter.on(
      "subscriberConnectFailed",
      ({ delay, error }) => {
        const generation = this.connectionEpoch;
        this.emit("error", error);
        if (generation !== this.connectionEpoch) {
          return;
        }
        const timer = setTimeout(() => {
          this.shardedSubscriberRetryTimers.delete(timer);
          refreshShardedSubscriberSlots(generation);
        }, delay);
        this.shardedSubscriberRetryTimers.add(timer);
      }
    );

    this.subscriberGroupEmitter.on("moved", () => {
      refreshShardedSubscriberSlots(this.connectionEpoch);
    });

    this.subscriberGroupEmitter.on("-subscriber", () => {
      this.emit("-subscriber");
    });

    this.subscriberGroupEmitter.on("+subscriber", () => {
      this.emit("+subscriber");
    });

    this.subscriberGroupEmitter.on("nodeError", (error, nodeKey) => {
      this.emit("nodeError", error, nodeKey);
    });

    this.subscriberGroupEmitter.on("subscribersReady", () => {
      this.emit("subscribersReady");
    });

    for (const event of ["smessage", "smessageBuffer"]) {
      this.subscriberGroupEmitter.on(event, (arg1, arg2, arg3) => {
        this.emit(event, arg1, arg2, arg3);
      });
    }
  }
}

interface Cluster extends EventEmitter {}
applyMixin(Cluster, EventEmitter);

addTransactionSupport(Cluster.prototype);
interface Cluster extends Transaction {}

export default Cluster;
