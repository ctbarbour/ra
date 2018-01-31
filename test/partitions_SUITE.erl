-module(partitions_SUITE).
-compile(export_all).

all() -> [publish_ack_10000].

publish_ack_10000(_Config) ->
    Nodes = start_erlang_cluster(5),
    Config = #{id => {publish_ack_10000, node()},
               uid => <<"publish_ack_10000">>,
               initial_nodes => [{publish_ack_10000, Node} || Node <- Nodes],
               log_module => ra_log_file,
               log_init_args => #{data_dir => "/tmp/partitions_SUITE/", uid => <<"publish_ack_10000">>},
               machine => {module, ra_fifo}
               },
    start_ra_cluster(Config, Nodes),
    NodeId = {publish_ack_10000, hd(Nodes)},
    start_consumer(NodeId),
    publish_n_messages(10000, NodeId, Nodes),
    print_metrics(Nodes),
    wait_for_consumption(10000, NodeId, Nodes),
    print_metrics(Nodes).

print_metrics(Nodes) ->
    [print_node_metrics(Node) || Node <- Nodes].

print_node_metrics(Node) ->
    ct:pal("Node ~p metrics ~p~n", [Node, rpc:call(Node, ets, tab2list, [ra_fifo_metrics])]).

start_consumer(NodeId) ->
    _ = ra:send_and_await_consensus(NodeId, {checkout, {auto, 10}, self()}).

publish_n_messages(MsgCount, NodeId, Nodes) ->
    [begin
        {ok, _, _} = ra:send_and_await_consensus(NodeId, {enqueue, Msg}),
        print_metrics(Nodes)
    end
     || Msg <- lists:seq(1, MsgCount)].

wait_for_consumption(MsgCount, NodeId, Nodes) ->
    [
    receive
        {ra_event, _, machine, {msg, MsgId, Msg}} ->
            case Msg rem 1000 of
                0 -> print_metrics(Nodes);
                _ -> ok
            end,
            {ok, _, _} = ra:send_and_await_consensus(NodeId, {settle, MsgId, self()})
    after 2000 ->
              error(ra_event_timeout)
    end
    || Msg <- lists:seq(1, 10000) ].


%% TODO: start N erlang nodes
start_erlang_cluster(5) ->
    Nodes = [foo1@localhost,
     foo2@localhost,
     foo3@localhost
     % ,
     % foo4@localhost,
     % foo5@localhost
     ],
    [start_node(Node) || Node <- Nodes],
    Nodes.

start_node(Node) ->
    Port = erlang:open_port({spawn, "erl -sname " ++ atom_to_list(Node)}, []),
    ct:pal("Start port ~p~n", [Port]),
    wait_for_distribution(Node, 50),
    add_lib_dir(Node),
    Node.

add_lib_dir(Node) ->
    rpc:call(Node, code, add_paths, [code:get_path()]).

wait_for_distribution(Node, 0) ->
    ct:pal("Distribution failed for node ~p nore more attempts", [Node]),
    error({distribution_failed_for, Node});
wait_for_distribution(Node, Attempts) ->
    ct:pal("Waiting for node ~p~n", [Node]),
    case rpc:call(Node, erlang, node, []) of
        Node -> ok;
        {badrpc, nodedown} ->
            timer:sleep(100),
            wait_for_distribution(Node, Attempts - 1)
    end.

start_ra_cluster(Config, Nodes) ->
    Configs = lists:map(fun(Node) ->
        ct:pal("Start app on ~p~n", [Node]),
        NodeConfig = make_node_config(Config, Node),
        ok = rpc:call(Node, application, load, [ra]),
        ok = rpc:call(Node, application, set_env, [ra, data_dir, filename:join("/tmp/partitions_SUITE/", atom_to_list(Node))]),
        {ok, _} = rpc:call(Node, application, ensure_all_started, [ra]),
        prepare_node(Node, NodeConfig),
        NodeConfig
    end,
    Nodes),
    lists:map(fun(#{id := {_, Node}} = NodeConfig) ->
        ct:pal("Start node on ~p~n", [Node]),
        ok = rpc:call(Node, ra, start_node, [NodeConfig]),
        NodeConfig
    end,
    Configs),
    ok = ra:trigger_election({publish_ack_10000, hd(Nodes)}).

make_node_config(#{id := {Id, _}, log_init_args := #{data_dir := DD, uid := Uid}} = Config, Node) ->
    Config#{
        id => {Id, Node},
        log_init_args => #{data_dir => filename:join(DD, atom_to_list(Node)),
                           uid => Uid}}.

prepare_node(Node, NodeConfig) ->
    ct:pal("Prepare node ~p~n", [Node]),
    spawn(Node,
          fun() ->
            ets:new(ra_fifo_metrics, [public, named_table, {write_concurrency, true}]),
            receive stop -> ok end
          end).

