"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const events_1 = require("events");
const utils_1 = require("../utils");
const util_1 = require("./util");
const Redis_1 = require("../Redis");
const debug = (0, utils_1.Debug)("cluster:connectionPool");
class ConnectionPool extends events_1.EventEmitter {
    constructor(redisOptions, clusterNodeRetryStrategy = null) {
        super();
        this.redisOptions = redisOptions;
        this.clusterNodeRetryStrategy = clusterNodeRetryStrategy;
        // master + slave = all
        this.nodeRecords = {
            all: {},
            master: {},
            slave: {},
        };
        this.specifiedOptions = {};
    }
    getNodes(role = "all") {
        const nodeRecords = this.nodeRecords[role];
        return Object.keys(nodeRecords).map((key) => { var _a; return (_a = nodeRecords[key]) === null || _a === void 0 ? void 0 : _a.redis; });
    }
    getInstanceByKey(key) {
        var _a;
        return (_a = this.nodeRecords.all[key]) === null || _a === void 0 ? void 0 : _a.redis;
    }
    getSampleInstance(role) {
        var _a;
        const keys = Object.keys(this.nodeRecords[role]);
        const sampleKey = (0, utils_1.sample)(keys);
        return (_a = this.nodeRecords[role][sampleKey]) === null || _a === void 0 ? void 0 : _a.redis;
    }
    /**
     * Add a master node to the pool
     * @param node
     */
    addMasterNode(node) {
        const key = (0, util_1.getNodeKey)(node.options);
        const redis = this.createRedisFromOptions(node, node.options.readOnly);
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
    createRedisFromOptions(node, readOnly) {
        const redis = new Redis_1.default((0, utils_1.defaults)({
            // By default, never try to reconnect when a node is lost,
            // instead, waiting for a `MOVED` error and fetching slots again.
            // When `clusterNodeRetryStrategy` is set, use it to allow
            // reconnection (e.g. for replica nodes that restart without
            // any slot changes).
            retryStrategy: typeof this.clusterNodeRetryStrategy === "function"
                ? this.clusterNodeRetryStrategy
                : null,
            // Offline queue should be enabled so that
            // we don't need to wait for the `ready` event
            // before sending commands to the node.
            enableOfflineQueue: true,
            readOnly: readOnly,
        }, node, this.redisOptions, { lazyConnect: true }));
        return redis;
    }
    /**
     * Find or create a connection to the node
     */
    findOrCreate(redisOptions, readOnly = false) {
        const key = (0, util_1.getNodeKey)(redisOptions);
        readOnly = Boolean(readOnly);
        if (this.specifiedOptions[key]) {
            Object.assign(redisOptions, this.specifiedOptions[key]);
        }
        else {
            this.specifiedOptions[key] = redisOptions;
        }
        let nodeRecord;
        if (this.nodeRecords.all[key]) {
            nodeRecord = this.nodeRecords.all[key];
            if (nodeRecord.redis.options.readOnly !== readOnly) {
                nodeRecord.redis.options.readOnly = readOnly;
                debug("Change role of %s to %s", key, readOnly ? "slave" : "master");
                nodeRecord.redis[readOnly ? "readonly" : "readwrite"]().catch(utils_1.noop);
                if (readOnly) {
                    delete this.nodeRecords.master[key];
                    this.nodeRecords.slave[key] = nodeRecord;
                }
                else {
                    delete this.nodeRecords.slave[key];
                    this.nodeRecords.master[key] = nodeRecord;
                }
            }
        }
        else {
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
    reset(nodes, generation) {
        debug("Reset with %O", nodes);
        if (generation !== undefined) {
            this.generation = generation;
        }
        const newNodes = {};
        nodes.forEach((node) => {
            const key = (0, util_1.getNodeKey)(node);
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
    createNodeRecord(redis, key) {
        const nodeRecord = {
            redis,
            endListener: () => {
                this.removeNode(key, nodeRecord);
            },
            errorListener: (error) => {
                this.emit("nodeError", error, key);
            },
        };
        return nodeRecord;
    }
    /**
     * Remove a node from the pool.
     */
    removeNode(key, expectedRecord) {
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
exports.default = ConnectionPool;
