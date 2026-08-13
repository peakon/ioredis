import Command from "../Command";
import { DEFAULT_REDIS_OPTIONS, RedisOptions } from "./RedisOptions";

type BlockingTimeoutOptions = Pick<
  RedisOptions,
  "blockingTimeout" | "blockingTimeoutGrace"
>;

export function getConfiguredBlockingTimeout(
  options?: BlockingTimeoutOptions
): number | undefined {
  const configuredTimeout = options?.blockingTimeout;
  return typeof configuredTimeout === "number" && configuredTimeout > 0
    ? configuredTimeout
    : undefined;
}

export default function getBlockingTimeoutInMs(
  command: Command,
  options?: BlockingTimeoutOptions
): number | undefined {
  if (!Command.checkFlag("BLOCKING_COMMANDS", command.name)) {
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
          (options?.blockingTimeoutGrace ??
            DEFAULT_REDIS_OPTIONS.blockingTimeoutGrace)
      : configuredTimeout;
  }

  return commandTimeout === null ? configuredTimeout : undefined;
}
