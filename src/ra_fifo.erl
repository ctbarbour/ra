-module(ra_fifo).

-behaviour(ra_machine).

-compile({no_auto_import, [apply/3]}).

-include("ra.hrl").

-export([
         init/1,
         apply/3,
         leader_effects/1,
         tick/2,
         overview/1,
         shadow_copy/1,
         size_test/2,
         perf_test/2,
         profile/1
        ]).

-type msg() :: term().
-type msg_id() :: non_neg_integer().
-type customer_tag() :: binary().
-type delivery() :: {delivery, customer_tag(), msg_id(), term()}.
% the entity that receives messages
% uniquely identifies a customer
-type customer_id() :: {customer_tag(), pid()}.

-type checkout_spec() :: {once | auto, Num :: non_neg_integer()} | get.

-type protocol() ::
    {enqueue, Msg :: msg()} |
    {checkout, Spec :: checkout_spec(), Customer :: customer_id()} |
    {settle, MsgId :: msg_id(), Customer :: customer_id()} |
    {return, MsgId :: msg_id(), Customer :: customer_id()}.

-type command() :: protocol() | ra_machine:builtin_command().
-type metrics() :: {Name :: atom(),
                    Enqueued :: non_neg_integer(),
                    CheckedOut :: non_neg_integer(),
                    Settled :: non_neg_integer(),
                    Returned :: non_neg_integer()}.

-define(METRICS_TABLE, ra_fifo_metrics).
-define(SHADOW_COPY_INTERVAL, 128).
% metrics tuple format:
% {Key, Enqueues, Checkouts, Settlements, Returns}

-record(customer,
        {checked_out = #{} :: #{msg_id() => {ra_index(), msg()}},
         next_msg_id = 0 :: msg_id(), % part of snapshot data
         num = 0 :: non_neg_integer(), % part of snapshot data
         % number of allocated messages
         % part of snapshot data
         seen = 0 :: non_neg_integer(),
         lifetime = once :: once | auto}).

-record(state, {name :: atom(),
                % unassigned messages
                messages = #{} :: #{ra_index() => msg()},
                enqueue_count = 0 :: non_neg_integer(),
                % master index of all enqueue raft indexes
                % ra_fifo_index so that can take the smallest
                % gb_trees were too slow for insert heavy workloads
                % ra_fifo_index can also be slow when calculating the smallest
                % index when there are large gaps
                ra_indexes = ra_fifo_index:empty() :: ra_fifo_index:state(),
                % defines the lowest index available in the messages map
                low_index :: ra_index() | undefined,
                % the raft index of the first enqueue operation that
                % contribute to the current state
                first_enqueue_raft_index :: ra_index() | undefined,
                % customers need to reflect customer state at time of snapshot
                % needs to be part of snapshot
                customers = #{} :: #{customer_id() => #customer{}},
                % customers that require further service are queued here
                % needs to be part of snapshot
                service_queue = queue:new() :: queue:queue(customer_id()),
                metrics :: metrics()
               }).

-opaque state() :: #state{}.

-export_type([protocol/0,
              delivery/0,
              state/0]).

-spec init(atom()) -> {state(), ra_machine:effects()}.
init(Name) ->
    {#state{name = Name,
            metrics = {Name, 0, 0, 0, 0}},
     [{metrics_table, ra_fifo_metrics, {Name, 0, 0, 0, 0}}]}.


incr_enqueue_count(#state{enqueue_count = C} = State)
 when C =:= ?SHADOW_COPY_INTERVAL ->
    {State#state{enqueue_count = 1}, shadow_copy(State)};
incr_enqueue_count(#state{enqueue_count = C} = State) ->
    {State#state{enqueue_count = C + 1}, undefined}.

% msg_ids are scoped per customer
% ra_indexes holds all raft indexes for enqueues currently on queue
-spec apply(ra_index(), command(), state()) ->
    {state(), ra_machine:effects()}.
apply(RaftIdx, {enqueue, Msg}, #state{ra_indexes = Indexes0,
                                      messages = Messages,
                                      first_enqueue_raft_index = FirstEnqueueIdx,
                                      low_index = Low} = State00) ->
    {State0, Shadow} = incr_enqueue_count(State00),
    Indexes = ra_fifo_index:append(RaftIdx, Shadow, Indexes0),
    State1 = State0#state{ra_indexes = Indexes,
                          messages = Messages#{RaftIdx => Msg},
                          first_enqueue_raft_index = min(RaftIdx, FirstEnqueueIdx),
                          low_index = min(RaftIdx, Low)},
    {State2, Effects, Num} = checkout(State1, []),
    State = incr_metrics(State2, {1, Num, 0, 0}),
    {State, Effects};
apply(RaftIdx, {settle, MsgId, CustomerId},
      #state{customers = Custs0} = State) ->
    case Custs0 of
        #{CustomerId := Cust0 = #customer{checked_out = Checked0}} ->
            case maps:take(MsgId, Checked0) of
                error ->
                    % null operation
                    % we must be recovering after a snapshot
                    % in this case it should not have any effect on the final
                    % state
                    {State, []};
                {{MsgRaftIdx, _}, Checked} ->
                    settle(RaftIdx, CustomerId, MsgRaftIdx,
                           Cust0, Checked, State)
            end;
        _ ->
            {State, []}
    end;
apply(_RaftIdx, {checkout, get, {Tag, Pid}}, #state{messages = M} = State0)
  when map_size(M) == 0 ->
    %% TODO do we need metric visibility of empty get requests?
    {State0, [{send_msg, Pid, {delivery, Tag, undefined, empty}}]};
apply(_RaftIdx, {checkout, get, {_Tag, Pid} = Customer}, State0) ->
    State1 = update_customer(Customer, {once, 1}, State0),
    {State2, Effects, Num} = checkout(State1, []),
    State = incr_metrics(State2, {0, Num, 0, 0}),
    {State, [{monitor, process, Pid} | Effects]};
apply(_RaftIdx, {checkout, Spec, {_Tag, Pid} = Customer}, State0) ->
    State1 = update_customer(Customer, Spec, State0),
    {State2, Effects, Num} = checkout(State1, []),
    State = incr_metrics(State2, {0, Num, 0, 0}),
    {State, [{monitor, process, Pid} | Effects]};
apply(_RaftId, {down, CustomerPid}, #state{customers = Custs0} = State0) ->
    % return checked out messages to main queue
    % Find the customers for the down pid
    DownCustomers = maps:keys(maps:filter(fun({_, P}, _)
                                                when P =:= CustomerPid ->
                                                  true;
                                             (_, _) -> false
                                          end, Custs0)),
    State = lists:foldl(
              fun(CustomerId, S0) ->
                      case maps:take(CustomerId, Custs0) of
                          {#customer{checked_out = Checked0}, Custs} ->
                              S1 = maps:fold(fun (_MsgId, {RaftId, Msg}, S) ->
                                                     return(RaftId, Msg, S)
                                             end, S0, Checked0),
                              S = incr_metrics(S1, {0, 0, 0,
                                                    maps:size(Checked0)}),
                              S#state{customers = Custs};
                          error ->
                              % already removed - do nothing
                              S0
                      end
              end, State0, DownCustomers),
    {State, []}.

-spec leader_effects(state()) -> ra_machine:effects().
leader_effects(#state{customers = Custs}) ->
    % return effects to monitor all current customers
    [{monitor, process, P} || {_, P} <- maps:keys(Custs)].


-spec tick(non_neg_integer(), state()) -> ra_machine:effects().
tick(_Ts, #state{metrics = Metrics}) ->
    [{mod_call, ets, insert, [?METRICS_TABLE, Metrics]}].

overview(#state{customers = Custs,
                ra_indexes = Indexes}) ->
    #{type => ?MODULE,
      num_customers => maps:size(Custs),
      num_messages => ra_fifo_index:size(Indexes)}.

%%% Internal

incr_metrics(#state{metrics = {N, E0, C0, S0, R0}} = State, {E, C, S, R}) ->
    State#state{metrics = {N, E0 + E, C0 + C, S0 + S, R0 + R}}.

settle(IncomingRaftIdx, CustomerId, MsgRaftIdx, Cust0, Checked,
       #state{customers = Custs0, service_queue = SQ0,
              ra_indexes = Indexes0} = State0) ->
    Cust = Cust0#customer{checked_out = Checked},
    {Custs, SQ, Effects0} = update_or_remove_sub(CustomerId, Cust, Custs0, SQ0),
    Indexes = ra_fifo_index:delete(MsgRaftIdx, Indexes0),
    {State1, Effects, NumChecked} =
        checkout(State0#state{customers = Custs,
                              ra_indexes = Indexes,
                              service_queue = SQ},
                 Effects0),
    State = incr_metrics(State1, {0, NumChecked, 1, 0}),
    update_first_enqueue_raft_index(IncomingRaftIdx,
                                    MsgRaftIdx, Effects,
                                    State).

update_first_enqueue_raft_index(IncomingRaftIdx, MsgRaftIdx, Effects,
                                #state{first_enqueue_raft_index = First,
                                       ra_indexes = Indexes} = State) ->
    case ra_fifo_index:size(Indexes) of
        0 ->
            % there are no messages on queue anymore
            % we can forward release_cursor all the way until
            % the last received command
            {State#state{first_enqueue_raft_index = undefined},
             [{release_cursor, IncomingRaftIdx, shadow_copy(State)} | Effects]};
        _ when First =:= MsgRaftIdx ->
            % the first_enqueue_raft_index can be fowarded to next
            % available message
            case ra_fifo_index:smallest(Indexes) of
                 {Smallest, undefined} ->
                    % no shadow taken, no release cursor increase
                    {State#state{first_enqueue_raft_index = Smallest},
                     Effects};
                 {Smallest, Shadow} ->
                    % we emit the last index _not_ to contribute to the
                    % current state - hence the -1
                    {State#state{first_enqueue_raft_index = Smallest},
                     [{release_cursor, Smallest - 1, Shadow} | Effects]}
            end;
        _ ->
            % first_enqueue_raft_index  cannot be forwarded
            {State, Effects}
    end.

return(RaftId, Msg, #state{messages = Messages,
                           % ra_indexes = Indexes,
                           low_index = Low0} = State0) ->
    % this should not affect the release cursor in any way
    State0#state{messages = maps:put(RaftId, Msg, Messages),
                 % ra_indexes = ra_fifo_index:enter(RaftId,  Indexes),
                 low_index = min(RaftId, Low0)}.


checkout(State, Effects) ->
    checkout0(checkout_one(State), Effects, 0).

checkout0({State, []}, Effects, Num) ->
    {State, lists:reverse(Effects), Num};
checkout0({State, Efxs}, Effects, Num) ->
    checkout0(checkout_one(State), Efxs ++ Effects, Num + 1).

checkout_one(#state{messages = Messages0,
                    low_index = LowIdx,
                    ra_indexes = Indexes,
                    service_queue = SQ0,
                    customers = Custs0} = State0) ->
    % messages are available
    case maps:take(LowIdx, Messages0) of
        {Msg, Messages} ->
            % there are customers waiting to be serviced
            case queue:out(SQ0) of
                {{value, {CTag, CPid} = CustomerId}, SQ1} ->
                    % process customer checkout
                    case maps:get(CustomerId, Custs0, undefined) of
                        #customer{checked_out = Checked0,
                                  next_msg_id = Next,
                                  seen = Seen} = Cust0 ->
                            Checked = maps:put(Next, {LowIdx, Msg}, Checked0),
                            Cust = Cust0#customer{checked_out = Checked,
                                                  next_msg_id = Next+1,
                                                  seen = Seen+1},
                            {Custs, SQ, []} = % we expect no effects
                                update_or_remove_sub(CustomerId, Cust, Custs0, SQ1),
                            State = State0#state{service_queue = SQ,
                                                 low_index = ra_fifo_index:next_key_after(LowIdx, Indexes),
                                                 messages = Messages,
                                                 customers = Custs},
                            {State, [{send_msg, CPid, {delivery, CTag, Next, Msg}}]};
                        undefined ->
                            % customer did not exist but was queued, recurse
                            checkout_one(State0#state{service_queue = SQ1})
                    end;
                _ ->
                    {State0, []}
            end;
        error ->
            {State0, []}
    end.


update_or_remove_sub(CustomerId, #customer{lifetime = once,
                                           checked_out = Checked,
                                           num = N, seen = N} = Cust,
                     Custs, ServiceQueue) ->
    case maps:size(Checked)  of
        0 ->
            % we're done with this customer
            {maps:remove(CustomerId, Custs), ServiceQueue,
             [{demonitor, CustomerId}]};
        _ ->
            % there are unsettled items so need to keep around
            {maps:update(CustomerId, Cust, Custs), ServiceQueue, []}
    end;
update_or_remove_sub(CustomerId, #customer{lifetime = once} = Cust,
                     Custs, ServiceQueue) ->
    {maps:update(CustomerId, Cust, Custs),
     uniq_queue_in(CustomerId, ServiceQueue), []};
update_or_remove_sub(CustomerId, #customer{lifetime = auto,
                                           checked_out = Checked,
                                           num = Num} = Cust,
                     Custs, ServiceQueue) ->
    case maps:size(Checked) < Num of
        true ->
            {maps:update(CustomerId, Cust, Custs),
             uniq_queue_in(CustomerId, ServiceQueue), []};
        false ->
            {maps:update(CustomerId, Cust, Custs), ServiceQueue, []}
    end.

uniq_queue_in(Key, Queue) ->
    % TODO: queue:member could surely be quite expensive, however the practical
    % number of unique customers may not be large enough for it to matter
    case queue:member(Key, Queue) of
        true ->
            Queue;
        false ->
            queue:in(Key, Queue)
    end.


update_customer(CustomerId, {Life, Num},
                #state{customers = Custs0,
                       service_queue = ServiceQueue0} = State0) ->
    Init = #customer{lifetime = Life, num = Num},
    Custs = maps:update_with(CustomerId,
                             fun(S) ->
                                     S#customer{lifetime = Life, num = Num}
                             end, Init, Custs0),
    ServiceQueue = maybe_queue_customer(CustomerId, maps:get(CustomerId, Custs),
                                        ServiceQueue0),

    State0#state{customers = Custs, service_queue = ServiceQueue}.

maybe_queue_customer(CustomerId, #customer{checked_out = Checked, num = Num},
                     ServiceQueue0) ->
    case maps:size(Checked) of
        Size when Size < Num ->
            % customerect needs service - check if already on service queue
            case queue:member(CustomerId, ServiceQueue0) of
                true ->
                    ServiceQueue0;
                false ->
                    queue:in(CustomerId, ServiceQueue0)
            end;
        _ ->
            ServiceQueue0
    end.


size_test(NumMsg, NumCust) ->
    EnqGen = fun(N) -> {N, {enqueue, N}} end,
    CustGen = fun(N) -> {N, {checkout, {auto, 100}, spawn(fun() -> ok end)}} end,
    S0 = run_log(1, NumMsg, EnqGen, init(size_test)),
    S = run_log(NumMsg, NumMsg + NumCust, CustGen, S0),
    S2 = S#state{ra_indexes = ra_fifo_index:map(fun(_, _) -> undefined end, S#state.ra_indexes)},
    {erts_debug:size(S), erts_debug:size(S2)}.

perf_test(NumMsg, NumCust) ->
    timer:tc(fun () ->
                     EnqGen = fun(N) -> {N, {enqueue, N}} end,
                     Pid = spawn(fun() -> ok end),
                     CustGen = fun(N) -> {N, {checkout, {auto, NumMsg}, Pid}} end,
                     SetlGen = fun(N) -> {N, {settle, N - NumMsg - NumCust - 1, Pid}} end,
                     S0 = run_log(1, NumMsg, EnqGen, element(1, init(size_test))),
                     S1 = run_log(NumMsg, NumMsg + NumCust, CustGen, S0),
                     _ = run_log(NumMsg, NumMsg + NumCust + NumMsg, SetlGen, S1),
                     ok
             end).

profile(File) ->
    GzFile = atom_to_list(File) ++ ".gz",
    lg:trace([ra_fifo, maps, queue, ra_fifo_index], lg_file_tracer,
             GzFile, #{running => false, mode => profile}),
    NumMsg = 10000,
    NumCust = 500,
    EnqGen = fun(N) -> {N, {enqueue, N}} end,
    Pid = spawn(fun() -> ok end),
    CustGen = fun(N) -> {N, {checkout, {auto, NumMsg}, Pid}} end,
    SetlGen = fun(N) -> {N, {settle, N - NumMsg - NumCust - 1, Pid}} end,
    S0 = run_log(1, NumMsg, EnqGen, element(1, init(size_test))),
    S1 = run_log(NumMsg, NumMsg + NumCust, CustGen, S0),
    _ = run_log(NumMsg, NumMsg + NumCust + NumMsg, SetlGen, S1),
    lg:stop().


run_log(Num, Num, _Gen, State) ->
    State;
run_log(Num, Max, Gen, State0) ->
    {_, E} = Gen(Num),
    run_log(Num+1, Max, Gen, element(1, apply(Num, E, State0))).

shadow_copy(#state{customers = Customers} = State) ->
    % creates a copy of the current state suitable for snapshotting
    State#state{messages = #{},
                ra_indexes = ra_fifo_index:empty(),
                low_index = undefined,
                first_enqueue_raft_index = undefined,
                % TODO: optimise
                % this is inefficient (from a memory use point of view)
                % as it creates a new tuple for every customer
                % even if they haven't changed instead we could just update a copy
                % of the last dehydrated state with the difference
                customers = maps:map(fun (_, V) -> V#customer{checked_out = #{}} end,
                                     Customers)
               }.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(assertEffect(EfxPat, Effects),
    ?assert(lists:any(fun (EfxPat) -> true;
                          (_) -> false
                      end, Effects))).

ensure_ets() ->
    case ets:info(ra_fifo_metrics) of
        undefined ->
            _ = ets:new(ra_fifo_metrics,
                        [public, named_table, {write_concurrency, true}]);
        _ ->
            ok
    end.

enq_enq_checkout_test() ->
    ensure_ets(),
    Cid = {<<"enq_enq_checkout_test">>, self()},
    {State1, _} = enq(1, first, element(1, init(test))),
    {State2, _} = enq(2, second, State1),
    {_State3, Effects} =
        apply(3, {checkout, {once, 2}, Cid}, State2),
    ?debugFmt("Effects ~p~n", [Effects]),
    ?assertEffect({monitor, _, _}, Effects),
    ok.

enq_enq_checkout_get_test() ->
    ensure_ets(),
    Cid = {<<"enq_enq_checkout_get_test">>, self()},
    {State1, _} = enq(1, first, element(1, init(test))),
    {State2, _} = enq(2, second, State1),
    {_State3, Effects} =
        apply(3, {checkout, get, Cid}, State2),
    ?assertEffect({send_msg, _,
                   {delivery, <<"enq_enq_checkout_get_test">>, 0, first}},
                  Effects),
    ok.

checkout_get_empty_test() ->
    ensure_ets(),
    Cid = {<<"checkout_get_empty_test">>, self()},
    State = element(1, init(test)),
    {_State2, Effects} =
        apply(1, {checkout, get, Cid}, State),
    ?assertEffect({send_msg, _,
                   {delivery, <<"checkout_get_empty_test">>, undefined, empty}},
                  Effects),
    ok.

release_cursor_test() ->
    ensure_ets(),
    Cid = {<<"release_cursor_test">>, self()},
    {State1, _} = enq(1, first, element(1, init(test))),
    {State2, _} = enq(2, second, State1),
    {State3, _} = check(Cid, 3, 10, State2),
    % no release cursor effect at this point
    {State4, []} = settle(Cid, 4, 1, State3),
    {_Final, Effects1} = settle(Cid, 5, 0, State4),
    % empty queue forwards release cursor all the way
    ?assertEffect({release_cursor, 5, _}, Effects1),
    ok.

checkout_enq_settle_test() ->
    ensure_ets(),
    Cid = {<<"checkout_enq_settle_test">>, self()},
    {State1, [{monitor, _, _}]} = check(Cid, 1, element(1, init(test))),
    {State2, Effects0} = enq(2, first, State1),
    ?assertEffect({send_msg, _,
                   {delivery, <<"checkout_enq_settle_test">>, 0, first}},
                  Effects0),
    {State3, []} = enq(3, second, State2),
    {_, _Effects} = settle(Cid, 4, 0, State3),
    % the release cursor is the smallest raft index that does not
    % contribute to the state of the application
    % ?assertEffect({release_cursor, 2, _}, Effects),
    ok.

down_customer_returns_unsettled_test() ->
    ensure_ets(),
    Cid = {<<"down_customer_returns_unsettled_test">>, self()},
    {State0, []} = enq(1, second, element(1, init(test))),
    {State1, [{monitor, process, Pid}, _Del]} = check(Cid, 2, State0),
    {State2, []} = apply(3, {down, Pid}, State1),
    {_State, Effects} = check(Cid, 4, State2),
    ?assertEffect({monitor, process, _}, Effects),
    ok.

completed_customer_yields_demonitor_effect_test() ->
    ensure_ets(),
    Cid = {<<"completed_customer_yields_demonitor_effect_test">>, self()},
    {State0, []} = enq(1, second, element(1, init(test))),
    {State1, [{monitor, process, _}, _Msg]} = check(Cid, 2, State0),
    {_, Effects} = settle(Cid, 3, 0, State1),
    ?assertEffect({demonitor, _}, Effects),
    % release cursor for empty queue
    ?assertEffect({release_cursor, 3, _}, Effects),
    ok.

tick_test() ->
    ensure_ets(),
    {State0, []} = enq(1, second, element(1, init(test))),
    [{mod_call, ets, insert, [?METRICS_TABLE, {test, 1, 0, 0, 0}]}] =
        tick(1, State0),
    ok.

release_cursor_snapshot_state_test() ->
    ensure_ets(),
    Tag = <<"release_cursor_snapshot_state_test">>,
    Cid = {Tag, self()},
    OthPid = spawn(fun () -> ok end),
    Commands = [
                {checkout, {auto, 5}, Cid},
                {enqueue, 0},
                {enqueue, 1},
                {settle, 0, Cid},
                {enqueue, 2},
                {settle, 1, Cid},
                {checkout, {auto, 4}, {<<"oth">>, OthPid}},
                {enqueue, 3},
                {enqueue, 4},
                {settle, 2, Cid},
                {settle, 3, Cid},
                {enqueue, 5},
                {settle, 0, <<"oth">>},
                {enqueue, 6},
                {settle, 4, <<"oth">>},
                {checkout, {once, 0}, {<<"oth">>, OthPid}}
              ],
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    {State, Effects} = run_log(element(1, init(help)), Entries),

    [begin
         Filtered = lists:dropwhile(fun({X, _}) when X =< SnapIdx -> true;
                                       (_) -> false
                                    end, Entries),
         {S, _} = run_log(SnapState, Filtered),
         % assert log can be restored from any release cursor index
         ?assert(S =:= State)
     end || {release_cursor, SnapIdx, SnapState} <- Effects],
    ok.

performance_test() ->
    ensure_ets(),
    % just under ~200ms on my machine [Karl]
    NumMsgs = 100000,
    {Taken, _} = perf_test(NumMsgs, 0),
    ?debugFmt("performance_test took ~p ms for ~p messages",
              [Taken / 1000, NumMsgs]),
    ok.

enq(Idx, Msg, State) ->
    apply(Idx, {enqueue, Msg}, State).

check(Cid, Idx, State) ->
    apply(Idx, {checkout, {once, 1}, Cid}, State).

check(Cid, Idx, Num, State) ->
    apply(Idx, {checkout, {once, Num}, Cid}, State).

settle(Cid, Idx, MsgId, State) ->
    apply(Idx, {settle, MsgId, Cid}, State).

run_log(InitState, Entries) ->
    lists:foldl(fun ({Idx, E}, {Acc0, Efx0}) ->
                        {Acc, Efx} = apply(Idx, E, Acc0),
                        {Acc, Efx0 ++ Efx}
                end, {InitState, []}, Entries).
-endif.

