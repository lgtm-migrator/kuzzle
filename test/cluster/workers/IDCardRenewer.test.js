'use strict';

const should = require('should');
const sinon = require('sinon');

const IDCardRenewer = require('../../../lib/cluster/workers/IDCardRenewer');
const Redis = require('../../../lib/service/cache/redis');

describe('ClusterIDCardRenewer', () => {
  describe('#init', () => {
    let idCardRenewer;

    beforeEach(() => {
      idCardRenewer = new IDCardRenewer();
    });

    it('should initialize a redis client', async () => {
      await idCardRenewer.init({
        redis: {
          config: {
            initTimeout: 42
          },
          name: 'foo'
        }
      });

      should(idCardRenewer.redis).be.instanceOf(Redis);
    });

    it('should init variable based on the given config', async () => {
      await idCardRenewer.init({
        redis: {
          config: {
            initTimeout: 42
          },
          name: 'foo'
        },
        nodeIdKey: 'nodeIdKey',
        refreshDelay: 666,
      });

      should(idCardRenewer.nodeIdKey).be.eql('nodeIdKey');
      should(idCardRenewer.refreshDelay).be.eql(666);
      should(idCardRenewer.refreshTimer).not.null();
      should(idCardRenewer.disposed).be.false();
    });

    it('should set an interval that will call the method renewIDCard', async () => {
      idCardRenewer.renewIDCard = sinon.spy();

      await idCardRenewer.init({
        redis: {
          config: {
            initTimeout: 42
          },
          name: 'foo'
        },
        nodeIdKey: 'nodeIdKey',
        refreshDelay: 0,
      });

      await new Promise((res) => setTimeout(res, 10));

      should(idCardRenewer.renewIDCard).be.called();
    });
  });

  describe('#renewIDCard', () => {
    let idCardRenewer;

    beforeEach(async () => {
      idCardRenewer = new IDCardRenewer();
      idCardRenewer.parentPort = {
        postMessage: sinon.stub(),
      };

      idCardRenewer.dispose = sinon.stub().resolves();

      await idCardRenewer.init({
        redis: {
          config: {initTimeout: 42},
          name: 'bar'
        },
        nodeIdKey: 'foo',
        refreshDelay: 100,
      });

      idCardRenewer.redis = {
        commands: {
          pexpire: sinon.stub().resolves(1),
          del: sinon.stub().resolves(),
        }
      };
    });

    it('should call pexpire to refresh the key expiration time', async () => {
      await idCardRenewer.renewIDCard();

      should(idCardRenewer.redis.commands.pexpire)
        .be.calledOnce()
        .and.be.calledWith(
          'foo',
          150
        );

      should(idCardRenewer.dispose).not.be.called();
      should(idCardRenewer.parentPort.postMessage).not.be.called();
    });

    it('should call the dispose method and notify the main thread that the node was too slow to refresh the ID Card', async () => {
      idCardRenewer.redis.commands.pexpire.resolves(0); // Failed to renew the ID Card before the key expired
      await idCardRenewer.renewIDCard();
      
      should(idCardRenewer.redis.commands.pexpire).be.called();

      should(idCardRenewer.dispose).be.called();
      should(idCardRenewer.parentPort.postMessage)
        .be.called()
        .and.be.calledWith({ error: 'Node too slow: ID card expired' });
    });

    it('should not do anything if already disposed', async () => {
      idCardRenewer.disposed = true;
      await idCardRenewer.renewIDCard();

      should(idCardRenewer.redis.commands.pexpire).not.be.called();
      should(idCardRenewer.dispose).not.be.called();
      should(idCardRenewer.parentPort.postMessage).not.be.called();
    });
  });

  describe('#dispose', () => {
    let idCardRenewer;

    beforeEach(async () => {
      idCardRenewer = new IDCardRenewer();

      await idCardRenewer.init({
        redis: {
          config: {initTimeout: 42},
          name: 'bar'
        },
        nodeIdKey: 'foo',
        refreshDelay: 100,
      });

      idCardRenewer.redis = {
        commands: {
          pexpire: sinon.stub().resolves(1),
          del: sinon.stub().resolves(),
        }
      };
    });

    it('should set disposed to true and delete the nodeIdKey inside redis when called', async () => {
      await idCardRenewer.dispose();

      should(idCardRenewer.redis.commands.del).be.calledWith('foo');
      should(idCardRenewer.disposed).be.true();
      should(idCardRenewer.refreshTimer).be.null();
    });

    it('should do nothing when already disposed', async () => {
      idCardRenewer.disposed = true;
      await idCardRenewer.dispose();

      should(idCardRenewer.redis.commands.del).not.be.called();
    });
  });
});