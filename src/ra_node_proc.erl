-module(ra_node_proc).

-behaviour(gen_statem).

-include("ra.hrl").

%% State functions
-export([leader/3,
         follower/3,
         await_condition/3,
         candidate/3]).

%% gen_statem callbacks
-export([
         init/1,
         format_status/2,
         handle_event/4,
         terminate/3,
         code_change/4,
         callback_mode/0
        ]).

%% API
-export([start_link/1,
         command/3,
         cast_command/2,
         query/3,
         state_query/2,
         trigger_election/1,
         ping/2
        ]).

-export([send_rpc/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_BROADCAST_TIME, 50).
-define(DEFAULT_ELECTION_MULT, 3).
-define(TICK_INTERVAL_MS, 1000).
-define(DEFAULT_STOP_FOLLOWER_ELECTION, false).
-define(DEFAULT_AWAIT_CONDITION_TIMEOUT, 30000).

-type correlation() :: term().

-type command_reply_mode() :: after_log_append |
                              await_consensus |
                              {notify_on_consensus, pid(), correlation()}.

-type query_fun() :: fun((term()) -> term()).

-type ra_command_type() :: '$usr' | '$ra_query' | '$ra_join' | '$ra_leave'
                           | '$ra_cluster_change'.

-type ra_command() :: {ra_command_type(), term(), command_reply_mode()}.

-type ra_leader_call_ret(Result) :: {ok, Result, Leader::ra_node_id()} |
                                    {error, term()} |
                                    {timeout, ra_node_id()}.

-type ra_cmd_ret() :: ra_leader_call_ret(ra_idxterm() | term()).

-type gen_statem_start_ret() :: {ok, pid()} | ignore | {error, term()}.

-type safe_call_ret(T) :: timeout | {error, noproc | nodedown} | T.

-type states() :: leader | follower | candiate | await_condition.

%% ra_event types
-type ra_event_reject_detail() :: {not_leader, Leader :: maybe(ra_node_id()),
                                   correlation()}.

-type ra_event_body() ::
    % used for notifying senders of the ultimate fate of their command
    % sent using ra:send_and_notify
    {applied, ra_node_id(), correlation()} |
    {rejected, ra_node_id(), ra_event_reject_detail()} |
    % used to send message side-effects emitted by the state machine
    {machine, ra_node_id(), term()}.

-type ra_event() :: {ra_event, ra_event_body()}.

-export_type([ra_leader_call_ret/1,
              ra_cmd_ret/0,
              safe_call_ret/1,
              ra_event_reject_detail/0,
              ra_event/0,
              ra_event_body/0]).

-record(state, {node_state :: ra_node:ra_node_state(),
                name :: atom(),
                broadcast_time = ?DEFAULT_BROADCAST_TIME :: non_neg_integer(),
                tick_timeout :: non_neg_integer(),
                monitors = #{} :: #{pid() => reference()},
                pending_commands = [] :: [{{pid(), any()}, term()}],
                leader_monitor :: reference() | undefined,
                await_condition_timeout :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(ra_node:ra_node_config()) -> gen_statem_start_ret().
start_link(Config = #{id := Id}) ->
    Name = ra_lib:ra_node_id_to_local_name(Id),
    gen_statem:start_link({local, Name}, ?MODULE, Config, []).

-spec command(ra_node_id(), ra_command(), timeout()) ->
    ra_cmd_ret().
command(ServerRef, Cmd, Timeout) ->
    leader_call(ServerRef, {command, Cmd}, Timeout).

-spec cast_command(ra_node_id(), ra_command()) -> ok.
cast_command(ServerRef, Cmd) ->
    gen_statem:cast(ServerRef, {command, Cmd}).

-spec query(ra_node_id(), query_fun(), dirty | consistent) ->
    {ok, {ra_idxterm(), term()}, ra_node_id()}.
query(ServerRef, QueryFun, dirty) ->
    gen_statem:call(ServerRef, {dirty_query, QueryFun});
query(ServerRef, QueryFun, consistent) ->
    % TODO: timeout
    command(ServerRef, {'$ra_query', QueryFun, await_consensus}, 5000).


%% used to query the raft state rather than the machine state
-spec state_query(ra_node_id(), all | members) ->  ra_leader_call_ret(term()).
state_query(ServerRef, Spec) ->
    leader_call(ServerRef, {state_query, Spec}, ?DEFAULT_TIMEOUT).

-spec trigger_election(ra_node_id()) -> ok.
trigger_election(ServerRef) ->
    gen_statem:cast(ServerRef, trigger_election).

-spec ping(ra_node_id(), timeout()) -> safe_call_ret({pong, states()}).
ping(ServerRef, Timeout) ->
    gen_statem_safe_call(ServerRef, ping, Timeout).

leader_call(ServerRef, Msg, Timeout) ->
    case gen_statem_safe_call(ServerRef, {leader_call, Msg},
                              {dirty_timeout, Timeout}) of
        {redirect, Leader} ->
            leader_call(Leader, Msg, Timeout);
        {error, _} = E ->
            E;
        timeout ->
            % TODO: formatted error message
            {timeout, ServerRef};
        Reply ->
            {ok, Reply, ServerRef}
    end.

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

init(Config0) when is_map(Config0) ->
    process_flag(trap_exit, true),
    Config = maps:merge(config_defaults(), Config0),
    {#{id := Id, uid := UId, cluster := Cluster} = NodeState,
     InitEffects} = ra_node:init(Config),
    Key = ra_lib:ra_node_id_to_local_name(Id),
    % ensure ra_directory has the new pid
    yes = ra_directory:register_name(UId, self(), Key),
    _ = ets:insert(ra_metrics, {Key, 0, 0}),
    % ensure each relevant node is connected
    Peers = maps:keys(maps:remove(Id, Cluster)),
    _ = lists:foreach(fun ({_, Node}) ->
                              net_kernel:connect_node(Node);
                          (_) -> node()
                      end, Peers),
    TickTime = maps:get(tick_timeout, Config),
    AwaitCondTimeout = maps:get(await_condition_timeout, Config),
    State0 = #state{node_state = NodeState, name = Key,
                    tick_timeout = TickTime,
                    await_condition_timeout = AwaitCondTimeout},
    ?INFO("~p ra_node_proc:init/1:~n~p~n",
          [Id, ra_node:overview(NodeState)]),
    {State, Actions0} = handle_effects(InitEffects, cast, State0),
    % New cluster starts should be coordinated and elections triggered
    % explicitly hence if this is a new one we wait here.
    % Else we set an election timer
    Actions = case ra_node:is_new(State#state.node_state) of
                  true ->
                      Actions0;
                  false ->
                      ?INFO("~p: is not new, setting election timeout.~n",
                            [Id]),
                      [election_timeout_action(follower, State) | Actions0]
              end,

    {ok, follower, State, set_tick_timer(State, Actions)}.

%% callback mode
callback_mode() -> state_functions.

%%%===================================================================
%%% State functions
%%%===================================================================

leader(EventType, {leader_call, Msg}, State) ->
    %  no need to redirect
    leader(EventType, Msg, State);
leader(EventType, {leader_cast, Msg}, State) ->
    leader(EventType, Msg, State);
leader(EventType, {command, {CmdType, Data, ReplyMode}},
       #state{node_state = NodeState0} = State0) ->
    %% Persist command into log
    %% Return raft index + term to caller so they can wait for apply
    %% notifications
    %% Send msg to peer with updated state data
    From = case EventType of
               cast -> undefined;
               {call, F} -> F
           end,
    {leader, NodeState, Effects} =
        ra_node:handle_leader({command, {CmdType, From, Data, ReplyMode}},
                              NodeState0),
    {State, Actions} = handle_effects(Effects, EventType,
                                      State0#state{node_state = NodeState}),
    {keep_state, State, Actions};
leader({call, From}, {dirty_query, QueryFun},
       #state{node_state = NodeState} = State) ->
    Reply = perform_dirty_query(QueryFun, leader, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
leader({call, From}, {state_query, Spec},
       #state{node_state = NodeState} = State) ->
    Reply = do_state_query(Spec, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
leader({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, leader}}]};
leader(info, {node_event, _Node, _Evt}, State) ->
    {keep_state, State};
leader(info, {'DOWN', MRef, process, Pid, _Info},
       #state{monitors = Monitors0,
              node_state = NodeState0} = State0) ->
    case maps:take(Pid, Monitors0) of
        {MRef, Monitors} ->
            % there is a monitor for the ref - create next_event action
            {leader, NodeState, Effects} =
                ra_node:handle_leader({command, {'$usr', Pid, {down, Pid},
                                                 after_log_append}},
                                      NodeState0),
            {State, Actions0} =
                handle_effects(Effects, call,
                               State0#state{node_state = NodeState,
                                            monitors = Monitors}),
            % remove replies
            % TODO: make this nicer
            Actions = lists:filter(fun({reply, _}) -> false;
                                      ({reply, _, _}) -> false;
                                      (_) -> true
                                   end, Actions0),
            {keep_state, State, Actions};
        error ->
            {keep_state, State0, []}
    end;
leader(_, tick_timeout, State0) ->
    State1 = maybe_persist_last_applied(send_rpcs(State0)),
    Effects = ra_node:tick(State1#state.node_state),
    {State, Actions} = handle_effects(Effects, cast, State1),
    {keep_state, State, set_tick_timer(State, Actions)};
leader(EventType, Msg, State0) ->
    case handle_leader(Msg, State0) of
        {leader, State1, Effects} ->
            {State, Actions} = handle_effects(Effects, EventType, State1),
            {keep_state, State, Actions};
        {follower, State1, Effects} ->
            {State, Actions} = handle_effects(Effects, EventType, State1),
            % demonitor when stepping down
            ok = lists:foreach(fun erlang:demonitor/1,
                               maps:values(State#state.monitors)),
            {next_state, follower, State#state{monitors = #{}},
             maybe_set_election_timeout(State, Actions)};
        {stop, State1, Effects} ->
            % interact before shutting down in case followers need
            % to know about the new commit index
            {State, _Actions} = handle_effects(Effects, EventType, State1),
            {stop, normal, State}
    end.

candidate({call, From}, {leader_call, Msg},
          State = #state{pending_commands = Pending}) ->
    {keep_state, State#state{pending_commands = [{From, Msg} | Pending]}};
candidate(cast, {command, {_CmdType, _Data, {notify_on_consensus, Corr, Pid}}},
         State) ->
    ok = reject_command(Pid, Corr, State),
    {keep_state, State, []};
candidate({call, From}, {dirty_query, QueryFun},
         #state{node_state = NodeState} = State) ->
    Reply = perform_dirty_query(QueryFun, candidate, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
candidate({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, candidate}}]};
candidate(info, {node_event, _Node, _Evt}, State) ->
    {keep_state, State};
candidate(_, tick_timeout, State0) ->
    State = maybe_persist_last_applied(State0),
    {keep_state, State, set_tick_timer(State, [])};
candidate(EventType, Msg, #state{pending_commands = Pending} = State0) ->
    case handle_candidate(Msg, State0) of
        {candidate, State1, Effects} ->
            {State, Actions0} = handle_effects(Effects, EventType, State1),
            Actions = case Msg of
                          election_timeout ->
                              [election_timeout_action(candidate, State)
                               | Actions0];
                          _ -> Actions0
                      end,
            {keep_state, State, Actions};
        {follower, State1, Effects} ->
            {State, Actions} = handle_effects(Effects, EventType, State1),
            ?INFO("~p candidate -> follower term: ~p~n",
                  [id(State), current_term(State)]),
            {next_state, follower, State,
             % always set an election timeout here to ensure an unelectable
             % node doesn't cause an electable one not to trigger another election
             % when not using follower timeouts
             % TODO: only set this is leader was not detected
             [election_timeout_action(candidate, State) | Actions]};
        {leader, State1, Effects} ->
            {State, Actions} = handle_effects(Effects, EventType, State1),
            ?INFO("~p candidate -> leader term: ~p~n",
                  [id(State), current_term(State)]),
            % inject a bunch of command events to be processed when node
            % becomes leader
            NextEvents = [{next_event, {call, F}, Cmd} || {F, Cmd} <- Pending],
            {next_state, leader, State, Actions ++ NextEvents}
    end.


follower({call, From}, {leader_call, Msg}, State) ->
    maybe_redirect(From, Msg, State);
follower(cast, {command, {_CmdType, _Data, {notify_on_consensus, Corr, Pid}}},
         State) ->
    ok = reject_command(Pid, Corr, State),
    {keep_state, State, []};
follower({call, From}, {dirty_query, QueryFun},
         #state{node_state = NodeState} = State) ->
    Reply = perform_dirty_query(QueryFun, follower, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
follower(_Type, trigger_election, State) ->
    {keep_state, State, [{next_event, cast, election_timeout}]};
follower({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, follower}}]};
follower(info, {'DOWN', MRef, process, _Pid, Info},
         #state{leader_monitor = MRef} = State) ->
    case Info of
        noconnection ->
            handle_leader_down(State);
        _ ->
            ?WARN("~p: Leader monitor down with ~p, setting election timeout~n",
                  [id(State), Info]),
            {keep_state, State#state{leader_monitor = undefined},
             [election_timeout_action(follower, State)]}
    end;
follower(info, {node_event, Node, down}, State) ->
    case leader_id(State) of
        {_, Node} ->
            ?WARN("~p: Leader node ~p may be down, setting election timeout",
                  [id(State), Node]),
            handle_leader_down(State);
        _ ->
            {keep_state, State}
    end;
follower(_, tick_timeout, State0) ->
    State = maybe_persist_last_applied(State0),
    {keep_state, State, set_tick_timer(State, [])};
follower(EventType, Msg, #state{await_condition_timeout = AwaitCondTimeout,
                                leader_monitor = MRef} = State0) ->
    case handle_follower(Msg, State0) of
        {follower, State1, Effects} ->
            {State2, Actions} = handle_effects(Effects, EventType, State1),
            State = follower_leader_change(State0, State2),
            {keep_state, State, maybe_set_election_timeout(State, Actions)};
        {candidate, State1, Effects} ->
            {State, Actions} = handle_effects(Effects, EventType, State1),
            ?INFO("~p follower -> candidate term: ~p~n",
                  [id(State), current_term(State)]),
            _ = stop_monitor(MRef),
            {next_state, candidate, State#state{leader_monitor = undefined},
             [election_timeout_action(candidate, State) | Actions]};
        {await_condition, State1, Effects} ->
            {State2, Actions} = handle_effects(Effects, EventType, State1),
            State = follower_leader_change(State0, State2),
            ?INFO("~p follower -> await_condition term: ~p~n",
                  [id(State), current_term(State)]),
            {next_state, await_condition, State,
             [{state_timeout, AwaitCondTimeout, await_condition_timeout} | Actions]}
    end.

await_condition({call, From}, {leader_call, Msg}, State) ->
    maybe_redirect(From, Msg, State);
await_condition({call, From}, {dirty_query, QueryFun},
                #state{node_state = NodeState} = State) ->
    Reply = perform_dirty_query(QueryFun, follower, NodeState),
    {keep_state, State, [{reply, From, Reply}]};
await_condition({call, From}, ping, State) ->
    {keep_state, State, [{reply, From, {pong, await_condition}}]};
await_condition(_Type, trigger_election, State) ->
    {keep_state, State, [{next_event, cast, election_timeout}]};
await_condition(info, {'DOWN', MRef, process, _Pid, _Info},
                State = #state{leader_monitor = MRef, name = Name}) ->
    ?WARN("~p: Leader monitor down. Setting election timeout.", [Name]),
    {keep_state, State#state{leader_monitor = undefined},
     [election_timeout_action(follower, State)]};
await_condition(info, {node_event, Node, down}, State) ->
    case leader_id(State) of
        {_, Node} ->
            ?WARN("~p: Node ~p might be down. Setting election timeout.",
                  [id(State), Node]),
            {keep_state, State, [election_timeout_action(follower, State)]};
        _ ->
            {keep_state, State}
    end;
await_condition(EventType, Msg, State0 = #state{node_state = NState,
                                                leader_monitor = MRef}) ->
    case handle_await_condition(Msg, State0) of
        {follower, State1, Effects} ->
            {State, Actions} = handle_effects(Effects, EventType, State1),
            NewState = follower_leader_change(State0, State),
            ?INFO("~p await_condition -> follower term: ~p~n",
                  [id(State), current_term(State)]),
            {next_state, follower, NewState,
             [{state_timeout, infinity, await_condition_timeout} |
              maybe_set_election_timeout(State, Actions)]};
        {candidate, State1, Effects} ->
            {State, Actions} = handle_effects(Effects, EventType, State1),
            ?INFO("~p follower -> candidate term: ~p~n",
                  [ra_node:id(NState), current_term(State1)]),
            _ = stop_monitor(MRef),
            {next_state, candidate, State#state{leader_monitor = undefined},
             [election_timeout_action(candidate, State) | Actions]};
        {await_condition, State1, []} ->
            {keep_state, State1, []}
    end.

handle_event(_EventType, EventContent, StateName, State) ->
    ?WARN("~p: handle_event unknown ~p~n", [id(State), EventContent]),
    {next_state, StateName, State}.

terminate(Reason, StateName,
          #state{name = Key,
                 node_state = NodeState} = State) ->
    ?WARN("ra: ~p terminating with ~p in state ~p~n",
          [id(State), Reason, StateName]),
    _ = ra_node:terminate(NodeState),
    _ = aten:unregister(Key),
    _ = ets:delete(ra_metrics, Key),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

format_status(Opt, [_PDict, StateName, #state{node_state = NS}]) ->
    [{id, ra_node:id(NS)},
     {opt, Opt},
     {raft_state, StateName},
     {ra_node_state, ra_node:overview(NS)}
    ].

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_leader(Msg, #state{node_state = NodeState0} = State) ->
    {NextState, NodeState, Effects} = ra_node:handle_leader(Msg, NodeState0),
    {NextState, State#state{node_state = NodeState}, Effects}.

handle_candidate(Msg, #state{node_state = NodeState0} = State) ->
    {NextState, NodeState, Effects} = ra_node:handle_candidate(Msg, NodeState0),
    {NextState, State#state{node_state = NodeState}, Effects}.

handle_follower(Msg, #state{node_state = NodeState0} = State) ->
    {NextState, NodeState, Effects} = ra_node:handle_follower(Msg, NodeState0),
    {NextState, State#state{node_state = NodeState}, Effects}.

handle_await_condition(Msg, #state{node_state = NodeState0} = State) ->
    {NextState, NodeState, Effects} = ra_node:handle_await_condition(Msg, NodeState0),
    {NextState, State#state{node_state = NodeState}, Effects}.

perform_dirty_query(QueryFun, leader, #{machine_state := MacState,
                                        last_applied := Last,
                                        id := Leader,
                                        current_term := Term}) ->
    {ok, {{Last, Term}, QueryFun(MacState)}, Leader};
perform_dirty_query(QueryFun, _StateName, #{machine_state := MacState,
                                            last_applied := Last,
                                            current_term := Term} = NodeState) ->
    Leader = maps:get(leader_id, NodeState, not_known),
    {ok, {{Last, Term}, QueryFun(MacState)}, Leader}.

% effect handler: either executes an effect or builds up a list of
% gen_statem 'Actions' to be returned.
handle_effects(Effects, EvtType, State0) ->
    lists:foldl(fun(Effect, {State, Actions}) ->
                        handle_effect(Effect, EvtType, State, Actions)
                end, {State0, []}, Effects).

% -spec handle_effect(ra_node:ra_effect(), term(), #state{}, list()) ->
%     {#state{}, list()}.
handle_effect({next_event, Evt}, EvtType, State, Actions) ->
    {State, [{next_event, EvtType, Evt} |  Actions]};
handle_effect({next_event, _Type, _Evt} = Next, _EvtType, State, Actions) ->
    {State, [Next | Actions]};
handle_effect({send_msg, To, Msg}, _EvtType, State, Actions) ->
    ok = send_ra_event(To, Msg, machine, State),
    {State, Actions};
handle_effect({notify, Who, Correlation}, _EvtType, State, Actions) ->
    ok = send_ra_event(Who, Correlation, applied, State),
    {State, Actions};
handle_effect({cast, To, Msg}, _EvtType, State, Actions) ->
    ok = gen_server:cast(To, Msg),
    {State, Actions};
handle_effect({reply, _From, _Reply} = Action, _EvtType, State, Actions) ->
    {State, [Action | Actions]};
handle_effect({reply, Reply}, {call, From}, State, Actions) ->
    {State, [{reply, From, Reply} | Actions]};
handle_effect({reply, _Reply}, _, _State, _Actions) ->
    exit(undefined_reply);
handle_effect({send_vote_requests, VoteRequests}, _EvtType, State, Actions) ->
    % transient election processes
    T = {dirty_timeout, 500},
    Me = self(),
    [begin
         _ = spawn(fun () -> Reply = gen_statem:call(N, M, T),
                             ok = gen_statem:cast(Me, Reply)
                   end)
     end || {N, M} <- VoteRequests],
    {State, Actions};
handle_effect({send_rpcs, _IsUrgent, Rpcs}, _EvtType, State0, Actions) ->
    % fully qualified use only so that we can mock it for testing
    % TODO: review / refactor
    [?MODULE:send_rpc(To, Rpc) || {To, Rpc} <- Rpcs],
    % TODO: record metrics for sending times
    {State0, Actions};
handle_effect({release_cursor, Index, MacState}, _EvtType,
              #state{node_state = NodeState0} = State, Actions) ->
    NodeState = ra_node:update_release_cursor(Index, MacState, NodeState0),
    {State#state{node_state = NodeState}, Actions};
handle_effect({monitor, process, Pid}, _EvtType,
              #state{monitors = Monitors} = State, Actions) ->
    case Monitors of
        #{Pid := _MRef} ->
            % monitor is already in place - do nothing
            {State, Actions};
        _ ->
            MRef = erlang:monitor(process, Pid),
            {State#state{monitors = Monitors#{Pid => MRef}}, Actions}
    end;
handle_effect({demonitor, Pid}, _EvtType,
              #state{monitors = Monitors0} = State, Actions) ->
    case maps:take(Pid, Monitors0) of
        {MRef, Monitors} ->
            true = erlang:demonitor(MRef),
            {State#state{monitors = Monitors}, Actions};
        error ->
            % ref not known - do nothing
            {State, Actions}
    end;
handle_effect({incr_metrics, Table, Ops}, _EvtType,
              State = #state{name = Key}, Actions) ->
    _ = ets:update_counter(Table, Key, Ops),
    {State, Actions};
handle_effect({mod_call, Mod, Fun, Args}, _EvtType,
              State, Actions) ->
    _ = erlang:apply(Mod, Fun, Args),
    {State, Actions};
handle_effect({metrics_table, Table, Record}, _EvtType,
              State, Actions) ->
    % this is a call - hopefully only done on init
    ok = ra_metrics_ets:make_table(Table),
    true = ets:insert(Table, Record),
    {State, Actions}.

send_rpcs(State0) ->
    {State, Rpcs} = make_rpcs(State0),
    % module call so that we can mock
    % TODO: review
    [ok = ?MODULE:send_rpc(To, Rpc) || {To, Rpc} <-  Rpcs],
    State.

make_rpcs(State) ->
    {NodeState, Rpcs} = ra_node:make_rpcs(State#state.node_state),
    {State#state{node_state = NodeState}, Rpcs}.

send_rpc(To, Msg) ->
    % fake gen cast - need to avoid any blocking delays here
    send(To, {'$gen_cast', Msg}).

send_ra_event(To, Msg, EvtType, State) ->
    Id = id(State),
    send(To, {ra_event, {EvtType, Id, Msg}}).

id(#state{node_state = NodeState}) ->
    ra_node:id(NodeState).

leader_id(#state{node_state = NodeState}) ->
    ra_node:leader_id(NodeState).

current_term(#state{node_state = NodeState}) ->
    ra_node:current_term(NodeState).

maybe_set_election_timeout(#state{leader_monitor = LeaderMon},
                           Actions) when LeaderMon =/= undefined ->
    % only when a leader is known should we cancel the election timeout
    [{state_timeout, infinity, election_timeout} | Actions];
maybe_set_election_timeout(State, Actions) ->
    [election_timeout_action(follower, State) | Actions].

election_timeout_action(follower, #state{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout * ?DEFAULT_ELECTION_MULT) + (Timeout * 2),
    {state_timeout, T, election_timeout};
election_timeout_action(candidate, #state{broadcast_time = Timeout}) ->
    T = rand:uniform(Timeout * ?DEFAULT_ELECTION_MULT) + (Timeout * 4),
    {state_timeout, T, election_timeout}.

% sets the tock timer for periodic actions such as sending stale rpcs
% or persisting the last_applied index
set_tick_timer(#state{tick_timeout = TickTimeout}, Actions) ->
    [{{timeout, tick}, TickTimeout, tick_timeout} | Actions].


follower_leader_change(Old, #state{pending_commands = Pending,
                                   leader_monitor = OldMRef} = New) ->
    OldLeader = leader_id(Old),
    case leader_id(New) of
        OldLeader ->
            % no change
            New;
        undefined ->
            New;
        NewLeader ->
            MRef = swap_monitor(OldMRef, NewLeader),
            LeaderNode = ra_lib:ra_node_id_node(NewLeader),
            ok = aten:register(LeaderNode),
            OldLeaderNode = ra_lib:ra_node_id_node(OldLeader),
            ok = aten:unregister(OldLeaderNode),
            % leader has either changed or just been set
            ?INFO("~p detected a new leader ~p in term ~p~n",
                  [id(New), NewLeader, current_term(New)]),
            [ok = gen_statem:reply(From, {redirect, NewLeader})
             || {From, _Data} <- Pending],
            New#state{pending_commands = [],
                      leader_monitor = MRef}
    end.

swap_monitor(MRef, L) ->
    stop_monitor(MRef),
    erlang:monitor(process, L).

stop_monitor(undefined) ->
    ok;
stop_monitor(MRef) ->
    erlang:demonitor(MRef),
    ok.

gen_statem_safe_call(ServerRef, Msg, Timeout) ->
    try
        gen_statem:call(ServerRef, Msg, Timeout)
    catch
         exit:{timeout, _} ->
            timeout;
         exit:{noproc, _} ->
            {error, noproc};
         exit:{{nodedown, _}, _} ->
            {error, nodedown}
    end.

do_state_query(all, State) -> State;
do_state_query(members, #{cluster := Cluster}) ->
    maps:keys(Cluster).

config_defaults() ->
    #{broadcast_time => ?DEFAULT_BROADCAST_TIME,
      tick_timeout => ?TICK_INTERVAL_MS,
      await_condition_timeout => ?DEFAULT_AWAIT_CONDITION_TIMEOUT,
      initial_nodes => []
     }.

handle_leader_down(#state{leader_monitor = Mon} = State) ->
    Leader = leader_id(State),
    % ping leader to check if up
    % TODO: this can be replaced by a pre-vote phase
    case ra_node_proc:ping(Leader, 100) of
        {pong, leader} ->
            % leader is not down
            ok = ra_lib:iter_maybe(Mon, fun erlang:demonitor/1),
            {keep_state,
             State#state{leader_monitor = monitor(process, Leader)},
             []};
        PingRes ->
            ?INFO("~p: Leader ~p appears down. Ping returned: ~p~n"
                  " Setting election timeout~n",
                  [id(State), Leader, PingRes]),
            {keep_state, State#state{leader_monitor = undefined},
             [election_timeout_action(follower, State)]}
    end.

maybe_redirect(From, Msg, #state{pending_commands = Pending} = State) ->
    case leader_id(State) of
        undefined ->
            ?WARN("~p leader call - leader not known. "
                  "Command will be forwarded once leader is known.~n",
                  [id(State)]),
            {keep_state,
             State#state{pending_commands = [{From, Msg} | Pending]}};
        LeaderId ->
            % ?INFO("~p follower leader call - redirecting to ~p ~n", [Id, LeaderId]),
            {keep_state, State, {reply, From, {redirect, LeaderId}}}
    end.

reject_command(Pid, Corr, State) ->
    LeaderId = leader_id(State),
    ?INFO("~p follower received leader command - rejecting to ~p ~n",
          [id(State), LeaderId]),
    send_ra_event(Pid, {not_leader, LeaderId, Corr}, rejected,
                       State).

maybe_persist_last_applied(#state{node_state = NS} = State) ->
     State#state{node_state = ra_node:persist_last_applied(NS)}.

send(To, Msg) ->
    % we do not want to block the ra node whilst attempting to set up
    % a TCP connection to a potentially down node.
    case erlang:send(To, Msg, [noconnect, nosuspend]) of
        ok ->
            ok;
        noconnect ->
            % TODO: we could try to reconnect here in a different processes
            % but probably best not from a performance point of view
            ok
    end.
