import { expect } from "chai";
import * as sinon from "sinon";
import Command from "../../../lib/Command";
import { Cluster } from "../../../lib";
import MockServer from "../../helpers/mock_server";

describe("cluster:blocking timeout", () => {
  afterEach(() => sinon.restore());

  it("uses the configured safety timeout for a finite offline command", () => {
    const cluster = new Cluster([], {
      lazyConnect: true,
      redisOptions: {
        blockingTimeout: 1000,
        blockingTimeoutGrace: 25,
      },
    });
    sinon.stub(cluster, "connect").resolves();
    const setBlockingTimeout = sinon.spy(
      Command.prototype,
      "setBlockingTimeout"
    );

    const pending = cluster.bzpopmin("queue", 0.05);
    pending.catch(() => undefined);

    try {
      expect(setBlockingTimeout.calledOnceWithExactly(1000)).to.equal(true);
    } finally {
      cluster.disconnect();
    }
  });

  it("caps a longer finite command at the configured safety timeout", () => {
    const cluster = new Cluster([], {
      lazyConnect: true,
      redisOptions: {
        blockingTimeout: 20,
        blockingTimeoutGrace: 5,
      },
    });
    sinon.stub(cluster, "connect").resolves();
    const setBlockingTimeout = sinon.spy(
      Command.prototype,
      "setBlockingTimeout"
    );

    const pending = cluster.bzpopmin("queue", 0.2);
    pending.catch(() => undefined);

    try {
      expect(setBlockingTimeout.calledOnceWithExactly(20)).to.equal(true);
    } finally {
      cluster.disconnect();
    }
  });

  it("uses the configured safety timeout for an indefinite offline command", () => {
    const cluster = new Cluster([], {
      lazyConnect: true,
      redisOptions: { blockingTimeout: 60 },
    });
    sinon.stub(cluster, "connect").resolves();
    const setBlockingTimeout = sinon.spy(
      Command.prototype,
      "setBlockingTimeout"
    );

    const pending = cluster.bzpopmin("queue", 0);
    pending.catch(() => undefined);

    try {
      expect(setBlockingTimeout.calledOnceWithExactly(60)).to.equal(true);
    } finally {
      cluster.disconnect();
    }
  });

  it("does not enable offline blocking deadlines without configuration", () => {
    const cluster = new Cluster([], { lazyConnect: true });
    sinon.stub(cluster, "connect").resolves();
    const setBlockingTimeout = sinon.spy(
      Command.prototype,
      "setBlockingTimeout"
    );

    const pending = cluster.bzpopmin("queue", 0);
    pending.catch(() => undefined);

    try {
      expect(setBlockingTimeout.called).to.equal(false);
    } finally {
      cluster.disconnect();
    }
  });

  it("never sends a blocking command after its offline deadline settles", async () => {
    const port = 30027;
    let blockingCommands = 0;
    new MockServer(
      port,
      (argv) => {
        if (argv[0] === "bzpopmin") {
          blockingCommands += 1;
        }
      },
      [[0, 16383, ["127.0.0.1", port]]]
    );
    let dnsCallback: Parameters<
      NonNullable<Cluster["options"]["dnsLookup"]>
    >[1];
    const cluster = new Cluster([{ host: "cluster.test", port }], {
      lazyConnect: true,
      enableReadyCheck: false,
      redisOptions: { blockingTimeout: 30 },
      dnsLookup(_hostname, callback) {
        dnsCallback = callback;
      },
    });

    const pending = cluster.bzpopmin("queue", 0);

    try {
      expect(await pending).to.equal(null);
      const connection = cluster.connect();
      dnsCallback(null, "127.0.0.1");
      await connection;
      expect(cluster.status).to.equal("ready");
      expect(blockingCommands).to.equal(0);
    } finally {
      cluster.disconnect();
    }
  });

  it("does not retry a blocking command after its deadline settles", async () => {
    const cluster = new Cluster([], {
      lazyConnect: true,
      retryDelayOnFailover: 30,
      redisOptions: { blockingTimeout: 10 },
    });
    sinon.stub(cluster, "connect").resolves();
    let sends = 0;
    const redis = {
      sendCommand(command: Command) {
        sends += 1;
        if (sends === 1) {
          command.reject(new Error("Connection is closed."));
        }
      },
    };
    sinon
      .stub(cluster["connectionPool"], "getSampleInstance")
      .returns(redis as any);

    const pending = cluster.bzpopmin("queue", 0);
    cluster.status = "ready";
    cluster["executeOfflineCommands"]();

    try {
      expect(await pending).to.equal(null);
      await new Promise((resolve) => setTimeout(resolve, 60));
      expect(sends).to.equal(1);
    } finally {
      cluster.disconnect();
    }
  });

  it("releases timed-out commands while the cluster remains offline", async () => {
    const cluster = new Cluster([], {
      lazyConnect: true,
      redisOptions: { blockingTimeout: 5 },
    });
    sinon.stub(cluster, "connect").resolves();

    const pending = Array.from({ length: 50 }, (_, index) =>
      cluster.bzpopmin(`queue:${index}`, 0)
    );

    try {
      expect(await Promise.all(pending)).to.eql(Array(50).fill(null));
      await new Promise((resolve) => setImmediate(resolve));
      expect(cluster["offlineQueue"]).to.have.length(0);
    } finally {
      cluster.disconnect();
    }
  });

  it("flushes remaining pipeline commands after an offline command times out", async () => {
    const cluster = new Cluster([], {
      lazyConnect: true,
      redisOptions: { blockingTimeout: 10 },
    });
    cluster.status = "reconnecting";
    cluster.slots = Array(16384).fill(["node"]);

    const result = cluster.pipeline().bzpopmin("queue", 0).get("queue").exec();

    await new Promise((resolve) => setTimeout(resolve, 20));
    await new Promise((resolve) => setImmediate(resolve));
    expect(cluster["offlineQueue"]).to.have.length(1);

    const socket = { write: sinon.spy() };
    const redis = {
      stream: socket,
      sendCommand(command: Command, stream) {
        stream.write(command.toWritable(socket));
        command.resolve("value");
      },
    };
    sinon
      .stub(cluster["connectionPool"], "getInstanceByKey")
      .returns(redis as any);
    cluster.status = "ready";
    cluster["executeOfflineCommands"]();

    try {
      expect(await result).to.eql([
        [null, null],
        [null, "value"],
      ]);
      expect(socket.write.calledOnce).to.equal(true);
    } finally {
      cluster.disconnect();
    }
  });
});
