-module(partitions_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [
            publish_ack_no_partition,
            publish_ack_node_leave,
            publish_ack_node_restart,
            publish_ack_inet_tcp_proxy_blocks_single_node,
            publish_ack_inet_tcp_proxy_blocks_partition
         ].

init_per_testcase(TestCase, Config) ->
    Config0 = enable_dist_proxy_manager(Config),
    Nodes = erlang_nodes(5),
    start_erlang(Nodes, Config0),
    enable_dist_proxy(Nodes, Config0),
    NodeId = setup_ra_cluster(TestCase, Nodes),
    ok = ra:trigger_election(NodeId),
    [{node_id, NodeId}, {nodes, Nodes} | Config0].

end_per_testcase(TestCase, Config) ->
    Nodes = ?config(nodes, Config),
    stop_erlang(Nodes),
    {ok, Cwd} = file:get_cwd(),
    Dir = filename:join(Cwd, "partitions_SUITE"),
    os:cmd("rm -rf " ++ Dir).

publish_ack_no_partition(Config) ->
    Nodes = ?config(nodes, Config),
    EveryFun = fun() -> print_metrics(Nodes) end,
    publish_and_consume(Config, 1000, 100, EveryFun, EveryFun).

publish_ack_node_leave(Config) ->
    NodeId = ?config(node_id, Config),
    Nodes = ?config(nodes, Config),
    LogFun = fun() -> print_metrics(Nodes) end,
    StopFun = fun() -> print_metrics(Nodes), stop_random_node(NodeId, Nodes) end,
    publish_and_consume(Config, 1000, 400, LogFun, StopFun).

publish_ack_node_restart(Config) ->
    {Name, _} = NodeId = ?config(node_id, Config),
    Nodes = ?config(nodes, Config),
    LogFun = fun() -> print_metrics(Nodes) end,
    RestartFun = fun() ->
                    print_metrics(Nodes),
                    restart_random_node(Name, NodeId, Nodes, Config)
                 end,
    publish_and_consume(Config, 1000, 100, LogFun, RestartFun).

publish_ack_inet_tcp_proxy_blocks_single_node(Config) ->
    NodeId = ?config(node_id, Config),
    Nodes = ?config(nodes, Config),
    LogFun = fun() -> print_metrics(Nodes) end,
    BlockFun = fun() ->
                   print_metrics(Nodes),
                   unblock_inet_tcp_proxy(NodeId, Nodes),
                   block_rabdom_partition_inet_tcp_proxy(NodeId, Nodes)
               end,
    publish_and_consume(Config, 1000, 100, LogFun, BlockFun).

publish_ack_inet_tcp_proxy_blocks_partition(Config) ->
    NodeId = ?config(node_id, Config),
    Nodes = ?config(nodes, Config),
    LogFun = fun() -> print_metrics(Nodes) end,
    BlockFun = fun() ->
                   print_metrics(Nodes),
                   unblock_inet_tcp_proxy(NodeId, Nodes),
                   block_random_node_inet_tcp_proxy(NodeId, Nodes)
               end,
    publish_and_consume(Config, 10000, 100, LogFun, BlockFun).

publish_and_consume(Config, Total, Every, EveryPublishFun, EveryConsumeFun) ->
    NodeId = ?config(node_id, Config),
    Nodes = ?config(nodes, Config),
    start_consumer(NodeId),
    publish_n_messages(Total, NodeId, Every, EveryPublishFun),
    print_metrics(Nodes),
    wait_for_consumption(Total, NodeId, Every, EveryConsumeFun),
    print_metrics(Nodes).

unblock_inet_tcp_proxy({_, _}, Nodes) ->
    [ allow_traffic_between(Node, OtherNode)
      || OtherNode <- Nodes,
         Node <- Nodes,
         OtherNode =/= Node ].

block_random_node_inet_tcp_proxy(NodeId, Nodes) ->
    TargetNode = get_random_node(NodeId, Nodes),
    ct:pal("Blocking node ~p~n", [TargetNode]),
    [ block_traffic_between(TargetNode, OtherNode)
      || OtherNode <- Nodes,
         OtherNode =/= TargetNode].

block_rabdom_partition_inet_tcp_proxy(NodeId, Nodes) ->
    Node1 = get_random_node(NodeId, Nodes),
    Node2 = get_random_node(NodeId, Nodes),

    %% Node1 and Node2 will be cut off from the cluster.
    [ block_traffic_between(Node1, OtherNode)
      || OtherNode <- Nodes,
         OtherNode =/= Node1,
         OtherNode =/= Node2 ],
    [ block_traffic_between(Node2, OtherNode)
      || OtherNode <- Nodes,
         OtherNode =/= Node1,
         OtherNode =/= Node2 ].

get_random_node({_, Node}, Nodes) ->
    PossibleNodes = Nodes -- [Node],
    lists:nth(rand:uniform(length(PossibleNodes)), PossibleNodes).

stop_random_node(NodeId, Nodes) ->
    TargetNode = get_random_node(NodeId, Nodes),
    stop_node(TargetNode).

stop_node(Node) ->
    ct:pal("Stopping node ~p~n", [Node]),
    ct_slave:stop(Node),
    wait_for_stop(Node, 100).

restart_random_node(Name, NodeId, Nodes, Config) ->
    RestartNode = get_random_node(NodeId, Nodes),
    ct:pal("Restarting node ~p~n", [RestartNode]),
    ct_rpc:call(RestartNode, init, stop, []),
    wait_for_stop(RestartNode, 100),
    start_node(RestartNode, Config),
    setup_ra_cluster(Name, [RestartNode]).

stop_erlang(Nodes) ->
    [stop_node(Node) || Node <- Nodes].

wait_for_stop(Node, 0) ->
    error({stop_failed_for, Node});
wait_for_stop(Node, Attempts) ->
    case ct_rpc:call(Node, erlang, node, []) of
        {badrpc, nodedown} -> ok;
        _ ->
            timer:sleep(100),
            wait_for_stop(Node, Attempts - 1)
    end.

setup_ra_cluster(Name, Nodes) ->
    {ok, Cwd} = file:get_cwd(),
    filelib:ensure_dir(filename:join(Cwd, "partitions_SUITE")),
    Config = #{id => {Name, nonode},
               uid => atom_to_binary(Name, utf8),
               initial_nodes => [{Name, Node} || Node <- Nodes],
               log_module => ra_log_file,
               log_init_args => #{data_dir => filename:join(Cwd, "partitions_SUITE"),
                                  uid => atom_to_binary(Name, utf8)},
               machine => {module, ra_fifo}
               },
    start_ra_cluster(Config, Nodes),
    NodeId = {Name, hd(Nodes)}.

print_metrics(Nodes) ->
    [print_node_metrics(Node) || Node <- Nodes].

print_node_metrics(Node) ->
    % ok.
    ct:pal("Node ~p metrics ~p~n", [Node, ct_rpc:call(Node, ets, tab2list, [ra_fifo_metrics])]).

start_consumer(NodeId) ->
    _ = ra:send_and_await_consensus(NodeId, {checkout, {auto, 50}, self()}, 10000).

publish_n_messages(MsgCount, NodeId, EveryN, EveryFun) ->
    spawn(fun() ->
        [begin
            {ok, _, _} = ra:send_and_await_consensus(NodeId, {enqueue, Msg}, 10000),
            case Msg rem EveryN of
                0 -> EveryFun();
                _ -> ok
            end
        end
         || Msg <- lists:seq(1, MsgCount)]
    end).

wait_for_consumption(MsgCount, NodeId, EveryN, EveryFun) ->
    [
    receive
        {ra_event, _, machine, {msg, MsgId, Msg}} ->
            case Msg rem EveryN of
                0 -> EveryFun();
                _ -> ok
            end,
            {ok, _, _} = ra:send_and_await_consensus(NodeId, {settle, MsgId, self()}, 10000)
    after 2000 ->
              error(ra_event_timeout)
    end
    || Msg <- lists:seq(1, MsgCount) ].


erlang_nodes(5) ->
    [
     foo1@localhost,
     foo2@localhost,
     foo3@localhost,
     foo4@localhost,
     foo5@localhost
     ].

start_erlang(Nodes, Config) ->
    [start_node(Node, Config) || Node <- Nodes],
    Nodes.

start_node(Node, Config) ->
    DistMod = ?config(erlang_dist_module, Config),
    StartArgs = case DistMod of
        undefined ->
            "";
        _ ->
            DistModS = atom_to_list(DistMod),
            DistModPath = filename:absname(
              filename:dirname(code:where_is_file(DistModS ++ ".beam"))),
            DistArg = re:replace(DistModS, "_dist$", "", [{return, list}]),
            "-pa \"" ++ DistModPath ++ "\" -proto_dist " ++ DistArg
    end,
    ct_slave:start(Node, [{erl_flags, StartArgs}]),
    wait_for_distribution(Node, 50),
    add_lib_dir(Node),
    Node.

add_lib_dir(Node) ->
    ct_rpc:call(Node, code, add_paths, [code:get_path()]).

wait_for_distribution(Node, 0) ->
    ct:pal("Distribution failed for node ~p nore more attempts", [Node]),
    error({distribution_failed_for, Node});
wait_for_distribution(Node, Attempts) ->
    ct:pal("Waiting for node ~p~n", [Node]),
    case ct_rpc:call(Node, erlang, node, []) of
        Node -> ok;
        {badrpc, nodedown} ->
            timer:sleep(100),
            wait_for_distribution(Node, Attempts - 1)
    end.

start_ra_cluster(Config, Nodes) ->
    {ok, Cwd} = file:get_cwd(),
    Configs = lists:map(fun(Node) ->
        ct:pal("Start app on ~p~n", [Node]),
        NodeConfig = make_node_config(Config, Node),
        ok = ct_rpc:call(Node, application, load, [ra]),
        ok = ct_rpc:call(Node, application, set_env, [ra, data_dir, filename:join([Cwd, "partitions_SUITE", atom_to_list(Node)])]),
        {ok, _} = ct_rpc:call(Node, application, ensure_all_started, [ra]),
        prepare_node(Node),
        NodeConfig
    end,
    Nodes),
    lists:map(fun(#{id := {_, Node}} = NodeConfig) ->
        ct:pal("Start ra node on ~p~n", [Node]),
        ok = ct_rpc:call(Node, ra, start_node, [NodeConfig]),
        NodeConfig
    end,
    Configs).

make_node_config(#{id := {Id, _}, log_init_args := #{data_dir := DD, uid := Uid}} = Config, Node) ->
    Config#{
        id => {Id, Node},
        log_init_args => #{data_dir => filename:join(DD, atom_to_list(Node)),
                           uid => Uid}}.

prepare_node(Node) ->
    ct:pal("Prepare node ~p~n", [Node]),
    spawn(Node,
          fun() ->
            ets:new(ra_fifo_metrics, [public, named_table, {write_concurrency, true}]),
            receive stop -> ok end
          end).

enable_dist_proxy_manager(Config) ->
    inet_tcp_proxy_manager:start(),
    rabbit_ct_helpers:set_config(Config,
      {erlang_dist_module, inet_proxy_dist}).

enable_dist_proxy(Nodes, Config) ->
    ManagerNode = node(),
    %% We first start the proxy process on all nodes, then we close the
    %% existing connection.
    %%
    %% If we do that in a single loop, i.e. start the proxy on node 1
    %% and disconnect it, then, start the proxy on node 2 and disconnect
    %% it, etc., there is a chance that the connection is reopened
    %% by a node where the proxy is still disabled. Therefore, that
    %% connection would bypass the proxy process even though we believe
    %% it to be enabled.
    Map = lists:map(
      fun(Node) ->
          {DistPort, ProxyPort} = ct_rpc:call(Node, ?MODULE, start_dist_proxy_on_node, [ManagerNode])
      end, Nodes),
    ok = lists:foreach(fun(Node) ->
        ok = ct_rpc:call(Node, application, set_env, [kernel, dist_and_proxy_ports_map, Map])
    end,
    Nodes),
    ok = lists:foreach(
      fun(Node) ->
          ok = ct_rpc:call(Node, ?MODULE, disconnect_from_other_nodes, [Nodes -- [Node]])
      end, Nodes),
    Config.

start_dist_proxy_on_node(ManagerNode) ->
    DistPort = get_dist_port(),
    ProxyPort = get_free_port(),
    ok = inet_tcp_proxy:start(ManagerNode, DistPort, ProxyPort),
    {DistPort, ProxyPort}.

disconnect_from_other_nodes(Nodes) ->
    ok = inet_tcp_proxy:reconnect(Nodes).

get_dist_port() ->
    [Self | _] = string:split(atom_to_list(node()), "@"),
    {ok, Names} = net_adm:names(),
    proplists:get_value(Self, Names).

get_free_port() ->
    {ok, Listen} = gen_tcp:listen(0, []),
    {ok, Port} = inet:port(Listen),
    gen_tcp:close(Listen),
    Port.

block_traffic_between(NodeA, NodeB) ->
    true = rpc:call(NodeA, inet_tcp_proxy, block, [NodeB]),
    true = rpc:call(NodeB, inet_tcp_proxy, block, [NodeA]),
    timer:sleep(100),
    {badrpc, Err} = rpc:call(NodeA, rpc, call, [NodeB, erlang, node, []], 1000),
    ct:pal("Blocked node ~p from ~p: result ~p", [NodeA, NodeB, Err]).

allow_traffic_between(NodeA, NodeB) ->
    true = rpc:call(NodeA, inet_tcp_proxy, allow, [NodeB]),
    true = rpc:call(NodeB, inet_tcp_proxy, allow, [NodeA]).

