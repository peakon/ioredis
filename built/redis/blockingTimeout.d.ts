import Command from "../Command";
import { RedisOptions } from "./RedisOptions";
declare type BlockingTimeoutOptions = Pick<RedisOptions, "blockingTimeout" | "blockingTimeoutGrace">;
export declare function getConfiguredBlockingTimeout(options?: BlockingTimeoutOptions): number | undefined;
export default function getBlockingTimeoutInMs(command: Command, options?: BlockingTimeoutOptions): number | undefined;
export {};
