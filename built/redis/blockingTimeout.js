"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getConfiguredBlockingTimeout = void 0;
const Command_1 = require("../Command");
const RedisOptions_1 = require("./RedisOptions");
function getConfiguredBlockingTimeout(options) {
    const configuredTimeout = options === null || options === void 0 ? void 0 : options.blockingTimeout;
    return typeof configuredTimeout === "number" && configuredTimeout > 0
        ? configuredTimeout
        : undefined;
}
exports.getConfiguredBlockingTimeout = getConfiguredBlockingTimeout;
function getBlockingTimeoutInMs(command, options) {
    var _a;
    if (!Command_1.default.checkFlag("BLOCKING_COMMANDS", command.name)) {
        return undefined;
    }
    const configuredTimeout = getConfiguredBlockingTimeout(options);
    if (configuredTimeout === undefined) {
        return undefined;
    }
    const commandTimeout = command.extractBlockingTimeout();
    if (typeof commandTimeout === "number") {
        return commandTimeout > 0
            ? commandTimeout +
                ((_a = options === null || options === void 0 ? void 0 : options.blockingTimeoutGrace) !== null && _a !== void 0 ? _a : RedisOptions_1.DEFAULT_REDIS_OPTIONS.blockingTimeoutGrace)
            : configuredTimeout;
    }
    return commandTimeout === null ? configuredTimeout : undefined;
}
exports.default = getBlockingTimeoutInMs;
