"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const utils_1 = require("../utils");
const Deque = require("denque");
const debug = (0, utils_1.Debug)("delayqueue");
/**
 * Queue that runs items after specified duration
 */
class DelayQueue {
    constructor() {
        this.queues = {};
        this.timeouts = {};
    }
    /**
     * Add a new item to the queue
     *
     * @param bucket bucket name
     * @param item function that will run later
     * @param options
     */
    push(bucket, item, options) {
        const callback = options.callback || process.nextTick;
        if (!this.queues[bucket]) {
            this.queues[bucket] = new Deque();
        }
        const queue = this.queues[bucket];
        queue.push(item);
        if (!this.timeouts[bucket]) {
            this.timeouts[bucket] = setTimeout(() => {
                callback(() => {
                    this.timeouts[bucket] = null;
                    this.execute(bucket);
                });
            }, options.timeout);
        }
    }
    flush() {
        for (const bucket of Object.keys(this.timeouts)) {
            if (this.timeouts[bucket]) {
                clearTimeout(this.timeouts[bucket]);
            }
        }
        this.timeouts = {};
        const queues = this.queues;
        this.queues = {};
        for (const bucket of Object.keys(queues)) {
            const queue = queues[bucket];
            while ((queue === null || queue === void 0 ? void 0 : queue.length) > 0) {
                queue.shift()();
            }
        }
    }
    execute(bucket) {
        const queue = this.queues[bucket];
        if (!queue) {
            return;
        }
        const { length } = queue;
        if (!length) {
            return;
        }
        debug("send %d commands in %s queue", length, bucket);
        this.queues[bucket] = null;
        while (queue.length > 0) {
            queue.shift()();
        }
    }
}
exports.default = DelayQueue;
