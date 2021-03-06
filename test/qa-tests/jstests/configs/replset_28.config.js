load("jstests/configs/standard_dump_targets.config.js");

var getToolTest;

(function() {
  getToolTest = function(name) {
    var toolTest = new ToolTest(name, null);

    var replTest = new ReplSetTest({
      name: 'tool_replset',
      nodes: 3,
      oplogSize: 5
    });

    var nodes = replTest.startSet();
    replTest.initiate();
    var master = replTest.getPrimary();

    toolTest.m = master;
    toolTest.db = master.getDB(name);
    toolTest.port = replTest.getPort(master);

    var oldStop = toolTest.stop;
    toolTest.stop = function() {
      replTest.stopSet();
      oldStop.apply(toolTest, arguments);
    };

    toolTest.isReplicaSet = true;

    return toolTest;
  };
})();

var getCommonToolArguments = function() {
  return [];
};
