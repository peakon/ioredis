import { expect } from "chai";
import { Socket } from "net";
import { AbortError } from "redis-errors";
import * as sinon from "sinon";
import { Cluster } from "../../../lib";
import MockServer from "../../helpers/mock_server";

describe("cluster:lifecycle", () => {
  it("connects immediately after a manual disconnect", async () => {
    const port = 30010;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);

    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
    });
    const errors: Error[] = [];
    cluster.on("error", (error) => errors.push(error));

    try {
      await cluster.connect();

      for (let i = 0; i < 5; i++) {
        cluster.disconnect();
        await cluster.connect();
        expect(await cluster.ping()).to.equal("OK");
      }

      const errorMessages = errors.map((error) => error.message);
      expect(errorMessages).not.to.include("Connection is aborted");
      expect(errorMessages).not.to.include(
        "None of startup nodes is available"
      );
    } finally {
      cluster.disconnect();
    }
  });

  it("shares a connection attempt between concurrent callers", async () => {
    const port = 30011;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);

    let dnsCallback: Parameters<
      NonNullable<Cluster["options"]["dnsLookup"]>
    >[1];
    let dnsLookups = 0;
    const cluster = new Cluster([{ host: "cluster.test", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
      dnsLookup(_hostname, callback) {
        dnsLookups += 1;
        dnsCallback = callback;
      },
    });

    const first = cluster.connect();
    const second = cluster.connect();
    const samePromise = first === second;
    second.catch(() => undefined);

    dnsCallback(null, "127.0.0.1");

    try {
      const results = await Promise.allSettled([first, second]);
      expect(samePromise).to.equal(true);
      expect(dnsLookups).to.equal(1);
      expect(results.map((result) => result.status)).to.eql([
        "fulfilled",
        "fulfilled",
      ]);
    } finally {
      cluster.disconnect();
    }
  });

  it("isolates a stale DNS result after disconnecting an empty pool", async () => {
    const port = 30012;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);

    const dnsCallbacks: Array<
      Parameters<NonNullable<Cluster["options"]["dnsLookup"]>>[1]
    > = [];
    const cluster = new Cluster([{ host: "cluster.test", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
      dnsLookup(_hostname, callback) {
        dnsCallbacks.push(callback);
      },
    });

    const first = cluster.connect();
    const firstResult = first.then(
      () => ({ status: "fulfilled" as const, error: undefined }),
      (error: Error) => ({ status: "rejected" as const, error })
    );

    cluster.disconnect();
    expect(cluster.status).to.equal("end");

    const second = cluster.connect();
    dnsCallbacks[1](null, "127.0.0.1");
    await second;

    dnsCallbacks[0](new Error("stale DNS failure"), undefined);
    await new Promise((resolve) => setImmediate(resolve));

    try {
      const result = await firstResult;
      expect(result.status).to.equal("rejected");
      expect(result.error.message).to.equal("Connection is closed.");
      expect(cluster.status).to.equal("ready");
      expect(await cluster.ping()).to.equal("OK");
    } finally {
      cluster.disconnect();
    }
  });

  it("prevents a stale slot refresh from completing a newer connection", async () => {
    const port = 30013;
    const currentSlots = [[0, 16383, ["127.0.0.1", port]]];
    const staleSlots = [[0, 16383, ["127.0.0.1", 39999]]];
    let heldSocket: Socket;
    let slotRequests = 0;
    let firstRequestReady: () => void;
    let secondRequestReady: () => void;
    const firstRequest = new Promise<void>((resolve) => {
      firstRequestReady = resolve;
    });
    const secondRequest = new Promise<void>((resolve) => {
      secondRequestReady = resolve;
    });
    const server = new MockServer(port, (argv, socket, flags) => {
      if (argv[0] !== "cluster" || argv[1] !== "SLOTS") {
        return;
      }

      slotRequests += 1;
      if (slotRequests === 1) {
        heldSocket = socket;
        flags.hang = true;
        firstRequestReady();
        return;
      }

      secondRequestReady();
      return currentSlots;
    });

    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
    });
    const first = cluster.connect();
    first.catch(() => undefined);
    await firstRequest;

    cluster.disconnect();
    const second = cluster.connect();
    await secondRequest;
    await second;

    server.write(heldSocket, staleSlots);
    await new Promise((resolve) => setImmediate(resolve));

    try {
      expect(slotRequests).to.equal(2);
      expect(cluster.status).to.equal("ready");
      expect(cluster.nodes().map((node) => node.options.port)).to.eql([port]);
      expect(await cluster.ping()).to.equal("OK");
    } finally {
      cluster.disconnect();
    }
  });

  it("lets manual shutdown cancel a scheduled recovery", async () => {
    let dnsCallback: Parameters<
      NonNullable<Cluster["options"]["dnsLookup"]>
    >[1];
    let dnsLookups = 0;
    const cluster = new Cluster([{ host: "cluster.test", port: 30014 }], {
      lazyConnect: true,
      clusterRetryStrategy: () => 10,
      dnsLookup(_hostname, callback) {
        dnsLookups += 1;
        dnsCallback = callback;
      },
    });
    const reconnecting = new Promise<void>((resolve) => {
      cluster.once("reconnecting", () => resolve());
    });

    const connection = cluster.connect();
    connection.catch(() => undefined);
    dnsCallback(new Error("DNS unavailable"), undefined);
    await reconnecting;

    cluster.disconnect();
    await new Promise((resolve) => setTimeout(resolve, 30));

    expect(dnsLookups).to.equal(1);
    expect(cluster.status).to.equal("end");
    expect(cluster["reconnectTimeout"]).to.equal(null);
  });

  it("clears a reconnect timeout when its recovery becomes obsolete", async () => {
    const cluster = new Cluster([], {
      lazyConnect: true,
      clusterRetryStrategy: () => 0,
    });
    cluster["handleCloseEvent"](undefined, cluster["connectionEpoch"]);
    cluster["manuallyClosing"] = true;

    try {
      await new Promise((resolve) => setTimeout(resolve, 5));
      expect(cluster["reconnectTimeout"]).to.equal(null);
    } finally {
      cluster.disconnect();
    }
  });

  it("does not emit end again when already disconnected", async () => {
    const cluster = new Cluster([], { lazyConnect: true });
    const events: string[] = [];
    cluster.on("disconnecting", () => events.push("disconnecting"));
    cluster.on("close", () => events.push("close"));
    cluster.on("end", () => events.push("end"));

    cluster.disconnect();
    await new Promise((resolve) => setImmediate(resolve));
    expect(events).to.eql(["disconnecting", "close", "end"]);

    cluster.disconnect();
    await new Promise((resolve) => setImmediate(resolve));
    expect(events).to.eql(["disconnecting", "close", "end"]);
  });

  it("applies the same immediate reconnect lifecycle to duplicates", async () => {
    const port = 30015;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);

    const source = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
    });
    const duplicate = source.duplicate();

    try {
      await duplicate.connect();
      duplicate.disconnect();
      await duplicate.connect();
      expect(await duplicate.ping()).to.equal("OK");
    } finally {
      source.disconnect();
      duplicate.disconnect();
    }
  });

  it("lets quit cancel a connection while DNS is pending", async () => {
    let dnsCallback: Parameters<
      NonNullable<Cluster["options"]["dnsLookup"]>
    >[1];
    const cluster = new Cluster([{ host: "cluster.test", port: 30016 }], {
      lazyConnect: true,
      dnsLookup(_hostname, callback) {
        dnsCallback = callback;
      },
    });
    const connection = cluster.connect();
    const connectionResult = connection.then(
      () => "fulfilled",
      () => "rejected"
    );

    expect(await cluster.quit()).to.equal("OK");
    dnsCallback(null, "127.0.0.1");
    await new Promise((resolve) => setImmediate(resolve));

    expect(await connectionResult).to.equal("rejected");
    expect(cluster.status).to.equal("end");
    expect(cluster.nodes()).to.have.length(0);
  });

  it("isolates a delayed lazy quit completion from a new connection", async () => {
    const port = 30017;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
    });

    await cluster.quit();
    await cluster.connect();
    await new Promise((resolve) => setImmediate(resolve));

    try {
      expect(cluster.status).to.equal("ready");
      expect(await cluster.ping()).to.equal("OK");
    } finally {
      cluster.disconnect();
    }
  });

  it("isolates a connected quit from a later node end", async () => {
    const port = 30018;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
    });

    await cluster.connect();
    const oldNode = cluster.nodes()[0];
    const disconnect = sinon.stub(oldNode, "disconnect");

    await cluster.quit();
    await cluster.connect();
    oldNode.emit("end");
    await new Promise((resolve) => setImmediate(resolve));

    try {
      expect(cluster.status).to.equal("ready");
      expect(cluster.nodes()[0]).not.to.equal(oldNode);
      expect(await cluster.ping()).to.equal("OK");
    } finally {
      disconnect.restore();
      oldNode.disconnect();
      cluster.disconnect();
    }
  });

  it("finishes closing when a node quit rejects during teardown", async () => {
    const port = 30028;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
    });
    const terminalEvents: string[] = [];
    cluster.on("close", () => terminalEvents.push("close"));
    cluster.on("end", () => terminalEvents.push("end"));

    await cluster.connect();
    sinon
      .stub(cluster.nodes()[0], "quit")
      .rejects(new AbortError("Command aborted due to connection close"));

    try {
      await expectRejectedWith(
        cluster.quit(),
        "Command aborted due to connection close"
      );
      await new Promise((resolve) => setImmediate(resolve));
      expect(cluster.status).to.equal("end");
      expect(cluster.nodes()).to.have.length(0);
      expect(terminalEvents).to.eql(["close", "end"]);
    } finally {
      cluster.disconnect();
    }
  });

  it("emits terminal events when reconnecting immediately after disconnect", async () => {
    const port = 30029;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
    });
    const events: string[] = [];
    for (const event of [
      "disconnecting",
      "close",
      "end",
      "connecting",
      "connect",
      "ready",
    ]) {
      cluster.on(event, () => events.push(event));
    }

    await cluster.connect();
    await new Promise((resolve) => setImmediate(resolve));
    events.length = 0;

    cluster.disconnect();
    const reconnect = cluster.connect();
    await reconnect;
    await new Promise((resolve) => setImmediate(resolve));

    try {
      expect(events).to.eql([
        "disconnecting",
        "close",
        "end",
        "connecting",
        "connect",
        "ready",
      ]);
    } finally {
      cluster.disconnect();
    }
  });

  it("ignores an old drain after reconnecting from a node removal event", async () => {
    const port = 30019;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
    });

    await cluster.connect();
    const oldNode = cluster.nodes()[0];
    let reconnect: Promise<void>;
    cluster.once("-node", () => {
      cluster.disconnect();
      reconnect = cluster.connect();
    });
    oldNode.disconnect();
    oldNode.emit("end");
    await reconnect;

    try {
      expect(cluster.status).to.equal("ready");
      expect(await cluster.ping()).to.equal("OK");
    } finally {
      cluster.disconnect();
    }
  });

  it("lets a later shutdown cancel a connection queued behind teardown", async () => {
    const port = 30020;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
    });

    await cluster.connect();
    const generation = ++cluster["connectionEpoch"];
    const teardown = cluster["beginTeardown"](generation);
    cluster.status = "disconnecting";
    const queuedConnect = cluster.connect();
    cluster["settleTeardown"](teardown);
    cluster.disconnect();

    await expectRejectedWith(queuedConnect, "Connection is closed.");
    await new Promise((resolve) => setImmediate(resolve));
    expect(cluster.status).to.equal("end");
    expect(cluster.nodes()).to.have.length(0);
  });

  it("binds a later queued connection to the latest teardown", async () => {
    const port = 30030;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
    });

    await cluster.connect();
    const firstGeneration = ++cluster["connectionEpoch"];
    const firstTeardown = cluster["beginTeardown"](firstGeneration);
    cluster.status = "disconnecting";
    const firstQueued = cluster.connect();
    firstQueued.catch(() => undefined);

    const secondGeneration = ++cluster["connectionEpoch"];
    const secondTeardown = cluster["beginTeardown"](secondGeneration);
    cluster.status = "disconnecting";
    const secondQueued = cluster.connect();
    secondQueued.catch(() => undefined);

    try {
      expect(secondQueued).not.to.equal(firstQueued);
      cluster["settleTeardown"](firstTeardown);
      await expectRejectedWith(firstQueued, "Connection is closed.");

      cluster["settleTeardown"](secondTeardown);
      await secondQueued;
      expect(cluster.status).to.equal("ready");
      expect(await cluster.ping()).to.equal("OK");
    } finally {
      cluster["settleTeardown"](firstTeardown);
      cluster["settleTeardown"](secondTeardown);
      await Promise.allSettled([firstQueued, secondQueued]);
      cluster.disconnect();
    }
  });

  it("preserves the ready-check failure reason for forced recovery", async () => {
    const port = 30021;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(
      port,
      (argv) => {
        if (argv[0] === "cluster" && argv[1] === "INFO") {
          return "cluster_state:fail";
        }
      },
      slotTable
    );
    let recoveryReason: Error;
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      lazyConnect: true,
      clusterRetryStrategy(_times, reason) {
        recoveryReason = reason;
        return null;
      },
    });

    const connection = cluster.connect();
    cluster.on("error", () => undefined);

    try {
      await expectRejectedWith(
        connection,
        "None of startup nodes is available"
      );
      expect(recoveryReason).to.be.instanceOf(Error);
      expect(recoveryReason.message).to.equal("Cluster state is fail");
      expect(cluster.status).to.equal("end");
    } finally {
      cluster.disconnect();
    }
  });

  it("ignores an old drain after a refresh callback starts a new lifecycle", async () => {
    const port = 30022;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
    });

    await cluster.connect();
    let reconnect: Promise<void>;
    cluster["activeRefresh"] = {
      generation: cluster["connectionEpoch"],
      callbacks: [
        () => {
          cluster.disconnect();
          reconnect = cluster.connect();
        },
      ],
      settled: false,
    };

    cluster.nodes()[0].emit("end");
    await reconnect;

    try {
      expect(cluster.status).to.equal("ready");
      expect(await cluster.ping()).to.equal("OK");
    } finally {
      cluster.disconnect();
    }
  });

  it("keeps public watchdog recovery idempotent during teardown", () => {
    const cluster = new Cluster([], { lazyConnect: true });
    const generation = cluster["connectionEpoch"];

    for (const status of ["reconnecting", "disconnecting"] as const) {
      cluster.status = status;
      cluster.disconnect(true);
      expect(cluster["connectionEpoch"]).to.equal(generation);
    }

    cluster.status = "ready";
    cluster.disconnect(true);
    expect(cluster["connectionEpoch"]).to.equal(generation + 1);
    cluster.disconnect(true);
    expect(cluster["connectionEpoch"]).to.equal(generation + 1);
    cluster.disconnect();
  });

  it("lets watchdog recovery interrupt a stalled ready check", async () => {
    const port = 30031;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    let readyCheckStarted: () => void;
    const readyCheck = new Promise<void>((resolve) => {
      readyCheckStarted = resolve;
    });
    new MockServer(
      port,
      (argv, _socket, flags) => {
        if (argv[0] === "cluster" && argv[1] === "INFO") {
          flags.hang = true;
          readyCheckStarted();
        }
      },
      slotTable
    );
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      lazyConnect: true,
      clusterRetryStrategy: null,
    });
    const connection = cluster.connect();
    connection.catch(() => undefined);
    await readyCheck;

    try {
      expect(cluster.status).to.equal("connect");
      cluster.disconnect(true);
      expect(cluster.status).to.equal("end");
      await expectRejectedWith(connection, "Connection is closed.");
    } finally {
      cluster.disconnect();
      await connection.catch(() => undefined);
    }
  });

  it("stops sharded subscriber work on terminal end", () => {
    const cluster = new Cluster([], {
      lazyConnect: true,
      clusterRetryStrategy: null,
      shardedSubscribers: true,
    });
    const stop = sinon.spy(cluster["shardedSubscribers"], "stop");
    const retryTimer = setTimeout(() => undefined, 10_000);
    cluster["shardedSubscriberRetryTimers"].add(retryTimer);

    try {
      cluster["handleCloseEvent"](undefined, cluster["connectionEpoch"]);
      expect(cluster.status).to.equal("end");
      expect(stop.calledOnce).to.equal(true);
      expect(cluster["shardedSubscriberRetryTimers"].size).to.equal(0);
    } finally {
      clearTimeout(retryTimer);
      cluster.disconnect();
    }
  });

  it("preserves assigned slots when a refresh omits an unassigned range", async () => {
    const port = 30032;
    const node = ["127.0.0.1", port];
    let slotTable = [[0, 16383, node]];
    new MockServer(port, (argv) => {
      if (argv[0] === "cluster" && argv[1] === "SLOTS") {
        return slotTable;
      }
    });
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
      shardedSubscribers: true,
    });

    await cluster.connect();
    slotTable = [
      [0, 8191, node],
      [8193, 16383, node],
    ];
    await new Promise<void>((resolve, reject) => {
      cluster.refreshSlotsCache((error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });

    try {
      expect(cluster.slots[8192]).to.eql([`127.0.0.1:${port}`]);
    } finally {
      cluster.disconnect();
    }
  });

  it("does not attribute a stale sharded failure to a newer lifecycle", async () => {
    const port = 30023;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
      shardedSubscribers: true,
    });

    await cluster.connect();
    let reconnect: Promise<void>;
    cluster.once("error", () => {
      cluster.disconnect();
      reconnect = cluster.connect();
    });
    cluster["subscriberGroupEmitter"].emit("subscriberConnectFailed", {
      delay: 0,
      error: new Error("stale subscriber failure"),
    });
    await reconnect;
    await new Promise((resolve) => setTimeout(resolve, 5));

    try {
      expect(cluster.status).to.equal("ready");
      expect(cluster["shardedSubscriberRetryTimers"].size).to.equal(0);
      expect(await cluster.ping()).to.equal("OK");
    } finally {
      cluster.disconnect();
    }
  });

  it("does not let a failed startup error listener reset a newer pool", async () => {
    const port = 30024;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    let rejectSlots = true;
    new MockServer(port, (argv) => {
      if (argv[0] === "cluster" && argv[1] === "SLOTS") {
        return rejectSlots ? new Error("CLUSTERDOWN") : slotTable;
      }
    });
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
      clusterRetryStrategy: null,
    });
    let reconnect: Promise<void>;
    cluster.once("error", () => {
      rejectSlots = false;
      cluster.disconnect();
      reconnect = cluster.connect();
    });

    const first = cluster.connect();
    await expectRejectedWith(first, "Connection is closed.");
    await reconnect;

    try {
      expect(cluster.status).to.equal("ready");
      expect(cluster.nodes()).to.have.length(1);
      expect(await cluster.ping()).to.equal("OK");
    } finally {
      cluster.disconnect();
    }
  });

  it("stops an old multi-node pool reset after shutdown from +node", async () => {
    const firstPort = 30025;
    const secondPort = 30026;
    new MockServer(firstPort);
    new MockServer(secondPort);
    const cluster = new Cluster(
      [
        { host: "127.0.0.1", port: firstPort },
        { host: "127.0.0.1", port: secondPort },
      ],
      {
        enableReadyCheck: false,
        lazyConnect: true,
        clusterRetryStrategy: null,
      }
    );
    cluster.once("+node", () => cluster.disconnect());

    await expectRejectedWith(cluster.connect(), "Connection is closed.");
    await new Promise((resolve) => setImmediate(resolve));

    expect(cluster.status).to.equal("end");
    expect(cluster.nodes()).to.have.length(0);
  });

  it("finishes teardown before invoking canceled refresh callbacks", async () => {
    const port = 30033;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
    });
    await cluster.connect();
    let callbackState: { status: string; nodes: number };
    cluster["activeRefresh"] = {
      generation: cluster["connectionEpoch"],
      callbacks: [
        () => {
          callbackState = {
            status: cluster.status,
            nodes: cluster.nodes().length,
          };
        },
      ],
      settled: false,
    };

    cluster.disconnect();

    expect(callbackState).to.equal(undefined);
    expect(cluster.status).to.equal("end");
    expect(cluster.nodes()).to.have.length(0);
    await new Promise((resolve) => setImmediate(resolve));
    expect(callbackState).to.eql({ status: "end", nodes: 0 });
  });

  it("does not start a refresh from a callback canceled by teardown", async () => {
    const port = 30034;
    const slotTable = [[0, 16383, ["127.0.0.1", port]]];
    new MockServer(port, undefined, slotTable);
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
    });
    await cluster.connect();
    const getInfoFromNode = sinon.stub(cluster as any, "getInfoFromNode");
    cluster["activeRefresh"] = {
      generation: cluster["connectionEpoch"],
      callbacks: [() => cluster.refreshSlotsCache()],
      settled: false,
    };

    cluster.disconnect();
    await new Promise((resolve) => setImmediate(resolve));

    expect(getInfoFromNode.called).to.equal(false);
    expect(cluster.status).to.equal("end");
    expect(cluster.nodes()).to.have.length(0);
  });

  it("does not apply a slot refresh after its lifecycle stops", async () => {
    const port = 30035;
    const currentSlots = [[0, 16383, ["127.0.0.1", port]]];
    const staleSlots = [[0, 16383, ["127.0.0.1", 39999]]];
    let heldSocket: Socket;
    let releaseRequest: () => void;
    const requestStarted = new Promise<void>((resolve) => {
      releaseRequest = resolve;
    });
    let slotRequests = 0;
    const server = new MockServer(port, (argv, socket, flags) => {
      if (argv[0] !== "cluster" || argv[1] !== "SLOTS") {
        return;
      }
      if (++slotRequests === 1) {
        return currentSlots;
      }
      heldSocket = socket;
      flags.hang = true;
      releaseRequest();
    });
    const cluster = new Cluster([{ host: "127.0.0.1", port }], {
      enableReadyCheck: false,
      lazyConnect: true,
    });
    await cluster.connect();
    const operation = {
      generation: cluster["connectionEpoch"],
      callbacks: [],
      settled: false,
    };
    cluster["activeRefresh"] = operation;
    const refreshResult = new Promise<Error | null | undefined>((resolve) => {
      cluster["getInfoFromNode"](cluster.nodes()[0], operation, resolve);
    });
    await requestStarted;

    cluster.status = "end";
    server.write(heldSocket, staleSlots);
    const error = await refreshResult;

    try {
      expect(error).to.be.instanceOf(Error);
      expect(error.message).to.equal("Connection is closed.");
      expect(cluster.nodes().map((node) => node.options.port)).to.eql([port]);
    } finally {
      cluster.disconnect();
    }
  });
});

async function expectRejectedWith(
  promise: Promise<unknown>,
  message: string
): Promise<void> {
  try {
    await promise;
    throw new Error("Expected promise to reject");
  } catch (error) {
    expect(error).to.be.instanceOf(Error);
    expect((error as Error).message).to.equal(message);
  }
}
