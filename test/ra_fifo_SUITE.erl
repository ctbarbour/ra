-module(ra_fifo_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% common ra_log tests to ensure behaviour is equivalent across
%% ra_log backends

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     ra_fifo_client_basics,
     ra_fifo_client_handles_reject_notification,
     leader_monitors_customer,
     follower_takes_over_monitor,
     node_is_deleted,
     node_restart_after_application_restart,
     restarted_node_does_not_reissue_side_effects,
     checkout_get_returns_value,
     ra_fifo_client_dequeue
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_group(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    _ = application:load(ra),
    ok = application:set_env(ra, data_dir, PrivDir),
    application:ensure_all_started(ra),
    Config.

end_per_group(_, Config) ->
    _ = application:stop(ra),
    Config.

init_per_testcase(TestCase, Config) ->
    NodeName2 = list_to_atom(atom_to_list(TestCase) ++ "2"),
    [
     {uid, atom_to_binary(TestCase, utf8)},
     {node_id, {TestCase, node()}},
     {uid2, atom_to_binary(NodeName2, utf8)},
     {node_id2, {NodeName2, node()}}
     | Config].

ra_fifo_client_basics(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    CustomerTag = UId,
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    FState0 = ra_fifo_client:init([NodeId]),
    {ok, FState1} = ra_fifo_client:checkout(CustomerTag, 1, FState0),
    % _ = ra:send_and_await_consensus(NodeId, {checkout, {auto, 10}, Cid}),

    ra_log_wal:force_roll_over(ra_log_wal),
    % create segment the segment will trigger a snapshot
    timer:sleep(1000),

    {ok, _Seq, FState2} = ra_fifo_client:enqueue(one, FState1),
    % process ra events
    FState3 = process_ra_event(FState2, 250),

    FState5 = receive
                  {ra_event, From, Evt} ->
                      case ra_fifo_client:handle_ra_event(From, Evt, FState3) of
                          {internal, _AcceptedSeqs, _FState4} ->
                              exit(unexpected_internal_event);
                          {{delivery, C, [{MsgId, _Msg}]}, FState4} ->
                              {ok, _, S} = ra_fifo_client:settle(C, [MsgId],
                                                                 FState4),
                              S
                      end
              after 5000 ->
                        exit(await_msg_timeout)
              end,

    % process settle applied notificaiton
    FState5b = process_ra_event(FState5, 250),
    _ = ra:stop_node(UId),
    _ = ra:restart_node(Conf),

    % give time to become leader
    timer:sleep(500),
    {ok, _, FState6} = ra_fifo_client:enqueue(two, FState5b),
    % process applied event
    FState6b = process_ra_event(FState6, 250),

    receive
        {ra_event, Frm, E} ->
            case ra_fifo_client:handle_ra_event(Frm, E, FState6b) of
                {internal, _, _FState7} ->
                    ct:pal("unexpected event ~p~n", [E]),
                    exit({unexpected_internal_event, E});
                {{delivery, Ctag, [{Mid, {_, two}}]}, FState7} ->
                    {ok, _, _S} = ra_fifo_client:return(Ctag, [Mid], FState7),
                    ok
            end
    after 2000 ->
              exit(await_msg_timeout)
    end,
    ra:stop_node(NodeId),
    ok.

ra_fifo_client_handles_reject_notification(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId1 = ?config(node_id, Config),
    NodeId2 = ?config(node_id2, Config),
    UId1 = ?config(uid, Config),
    CId = {UId1, self()},
    UId2 = ?config(uid2, Config),
    Conf1 = conf(UId1, NodeId1, PrivDir, [NodeId1, NodeId2]),
    Conf2 = conf(UId2, NodeId2, PrivDir, [NodeId1, NodeId2]),
    _ = ra:start_node(Conf1),
    _ = ra:start_node(Conf2),
    ok = ra:trigger_election(NodeId1),
    _ = ra:send_and_await_consensus(NodeId1, {checkout, {auto, 10}, CId}),
    % reverse order - should try the first node in the list first
    F0 = ra_fifo_client:init([NodeId2, NodeId1]),
    {ok, _Seq, F1} = ra_fifo_client:enqueue(one, F0),

    timer:sleep(500),

    % the applied notification
    _F2 = process_ra_event(F1, 250),
    ra:stop_node(NodeId1),
    ra:stop_node(NodeId2),
    ok.

leader_monitors_customer(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Cid = {UId, self()},
    Name = element(1, NodeId),
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    _ = ra:send_and_await_consensus(NodeId, {checkout, {auto, 10}, Cid}),
    {monitored_by, [MonitoredBy]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredBy =:= whereis(Name)),
    ra:stop_node(UId),
    _ = ra:restart_node(Conf),
    % check monitors are re-applied after restart
    {ok, _, _} = ra:send_and_await_consensus(NodeId, {enqueue, msg1}),
    {monitored_by, [MonitoredByAfter]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredByAfter =:= whereis(Name)),
    ra:stop_node(NodeId),
    ok.

follower_takes_over_monitor(Config) ->
    PrivDir = ?config(priv_dir, Config),
    {Name1, _} = NodeId1 = ?config(node_id, Config),
    {Name2, _} = NodeId2 = ?config(node_id2, Config),
    UId1 = ?config(uid, Config),
    CId = {UId1, self()},
    UId2 = ?config(uid2, Config),
    Conf1 = conf(UId1, NodeId1, PrivDir, [NodeId1, NodeId2]),
    Conf2 = conf(UId2, NodeId2, PrivDir, [NodeId1, NodeId2]),
    _ = ra:start_node(Conf1),
    _ = ra:start_node(Conf2),
    ok = ra:trigger_election(NodeId1),
    _ = ra:send_and_await_consensus(NodeId1, {checkout, {auto, 10}, CId}),
    timer:sleep(500),
    {monitored_by, [MonitoredBy]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredBy =:= whereis(Name1)),

    ok = ra:trigger_election(NodeId2),
    {ok, _, NodeId2} = ra:send_and_await_consensus(NodeId2,
                                                   {enqueue, msg1}),

    {monitored_by, [MonitoredByAfter]} = erlang:process_info(self(), monitored_by),
    ?assert(MonitoredByAfter =:= whereis(Name2)),
    ra:stop_node(NodeId1),
    ra:stop_node(NodeId2),
    ok.

node_is_deleted(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    CId = {UId, self()},
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    {ok, _, NodeId} = ra:send_and_await_consensus(NodeId, {enqueue, msg1}),
    % force roll over
    ok = ra_log_wal:force_roll_over(ra_log_wal),
    ok = ra:delete_node(NodeId),

    % start a node with the same nodeid but different uid
    % simulatin the case where a queue got deleted then re-declared shortly
    % afterwards
    UId2 = ?config(uid2, Config),
    ok = ra:start_node(Conf#{uid => UId2,
                             log_init_args => #{data_dir => PrivDir,
                                                uid => UId2}}),
    ok = ra:trigger_election(NodeId),
    {ok, _, _} = ra:send_and_await_consensus(NodeId,
                                             {checkout, {auto, 10}, CId}),
    receive
        {ra_event, _, Evt} ->
            exit({unexpected_machine_event, Evt})
    after 500 -> ok
    end,
    ok = ra:delete_node(NodeId),
    ok.

node_restart_after_application_restart(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    {ok, _, NodeId} = ra:send_and_await_consensus(NodeId, {enqueue, msg1}),
    application:stop(ra),
    application:start(ra),
    % restart node
    ok = ra:restart_node(Conf),
    {ok, _, NodeId} = ra:send_and_await_consensus(NodeId, {enqueue, msg2}),
    ok = ra:stop_node(NodeId),
    ok.


% NB: this is not guaranteed not to re-issue side-effects but only tests
% that the likelyhood is small
restarted_node_does_not_reissue_side_effects(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    CId = {UId, self()},
    Name = element(1, NodeId),
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    {ok, _, _} = ra:send_and_await_consensus(NodeId, {checkout, {auto, 10}, CId}),
    {ok, _, _} = ra:send_and_await_consensus(NodeId, {enqueue, msg1}),
    receive
        {ra_event, _, {machine, {delivery, C, [{MsgId, _}]}}} ->
            {ok, _, _} = ra:send_and_await_consensus(NodeId, {settle, MsgId, C})
    after 2000 ->
              exit(ra_fifo_event_timeout)
    end,
    % give the process time to persist the last_applied index
    timer:sleep(1000),
    % kill the process and have it restarted by the supervisor
    exit(whereis(Name), kill),

    %  check message isn't received again
    receive
        {ra_event, _, {machine, {delivery, _, _}}} ->
            exit(unexpected_ra_fifo_event)
    after 1000 ->
              ok
    end,
    ok = ra:stop_node(UId),
    ok.

checkout_get_returns_value(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    CId = {UId, self()},
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    % nothing on queue yet
    {ok, {get, empty}, NodeId} = ra:send_and_await_consensus(
                                   NodeId, {checkout, {get, settled}, CId}),
    {ok, _, _} = ra:send_and_await_consensus(NodeId, {enqueue, msg1}),
    {ok, {get, {0, {_, msg1}}}, NodeId} = ra:send_and_await_consensus(
                                       NodeId, {checkout, {get, settled}, CId}),
    {ok, _, _} = ra:send_and_await_consensus(NodeId, {enqueue, msg2}),
    {ok, {get, {0, {_, msg2}}}, NodeId} = ra:send_and_await_consensus(
                                       NodeId, {checkout, {get, unsettled}, CId}),
    {ok, _, _} = ra:send_and_await_consensus(NodeId, {settle, 0, CId}),
    ok.

ra_fifo_client_dequeue(Config) ->
    PrivDir = ?config(priv_dir, Config),
    NodeId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Tag = UId,
    Conf = #{id => NodeId,
             uid => UId,
             log_module => ra_log_file,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_nodes => [],
             machine => {module, ra_fifo}},
    _ = ra:start_node(Conf),
    ok = ra:trigger_election(NodeId),
    F1 = ra_fifo_client:init([NodeId]),
    {ok, empty, F1b} = ra_fifo_client:dequeue(Tag, settled, F1),
    {ok, 0, F2} = ra_fifo_client:enqueue(msg1, F1b),
    {ok, {0, {_, msg1}}, F3} = ra_fifo_client:dequeue(Tag, settled, F2),
    {ok, 1, F4} = ra_fifo_client:enqueue(msg2, F3),
    {ok, {MsgId, {_, msg2}}, F5} = ra_fifo_client:dequeue(Tag, unsettled, F4),
    {ok, _, F6} = ra_fifo_client:settle(Tag, [MsgId], F5),
    ct:pal("F6 ~p~n", [F6]),
    ok.

conf(UId, NodeId, Dir, Peers) ->
    #{id => NodeId,
      uid => UId,
      log_module => ra_log_file,
      log_init_args => #{data_dir => Dir, uid => UId},
      initial_nodes => Peers,
      machine => {module, ra_fifo}}.

process_ra_event(State, Wait) ->
    receive
        {ra_event, From, Evt} ->
            ct:pal("processed ra event ~p~n", [Evt]),
            {internal, _, S} = ra_fifo_client:handle_ra_event(From, Evt, State),
            S
    after Wait ->
              exit(ra_event_timeout)
    end.

process_ra_events(State0, Wait) ->
    receive
        {ra_event, From, Evt} ->
            {internal, _, State} = ra_fifo_client:handle_ra_event(From, Evt, State0),
            process_ra_event(State, Wait)
    after Wait ->
              State0
    end.
