import { EventEmitter } from "events";
import { expect } from "chai";
import * as sinon from "sinon";
import ClusterSubscriberGroup from "../../../lib/cluster/ClusterSubscriberGroup";
import ShardedSubscriber from "../../../lib/cluster/ShardedSubscriber";

describe("ClusterSubscriberGroup lifecycle", () => {
  afterEach(() => sinon.restore());

  it("drops an in-flight subscriber failure after stop", async () => {
    let rejectStart: (error: Error) => void;
    sinon.stub(ShardedSubscriber.prototype, "isLazyConnect").returns(false);
    sinon.stub(ShardedSubscriber.prototype, "start").callsFake(
      () =>
        new Promise<void>((_resolve, reject) => {
          rejectStart = reject;
        })
    );

    const emitter = new EventEmitter();
    const connectFailed = sinon.spy();
    const subscribersReady = sinon.spy();
    emitter.on("subscriberConnectFailed", connectFailed);
    emitter.on("subscribersReady", subscribersReady);
    const group = new ClusterSubscriberGroup(emitter, {});

    const reset = group.reset(
      [["127.0.0.1:30001"]],
      [{ options: { host: "127.0.0.1", port: 30001 } }]
    );
    group.stop();
    rejectStart(new Error("late subscriber failure"));
    await reset;

    expect(connectFailed.called).to.equal(false);
    expect(subscribersReady.called).to.equal(false);
  });
});
