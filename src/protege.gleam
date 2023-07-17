import gleam/io
import gleam/result
import gleam/option.{None, Option, Some}
import gleam/order.{Gt}
import gleam/erlang/atom.{Atom}
import gleam/erlang/process.{
  CallError, Pid, Subject, flush_messages, self, send as process_send,
  subject_owner, try_call as process_try_call,
}
import gleam/otp/actor.{Continue, Next, StartError, Stop, start as otp_start}
import birl/time.{DateTime, add, compare, now}
import birl/duration.{Duration}

pub type OverloadProtection {
  OverloadProtection(
    sync_limit: Int,
    drop_limit: Int,
    flush_limit: Int,
    burst_protection: Bool,
    burst_window: Duration,
    burst_limit: Int,
    sync_timeout: Int,
  )
}

pub type ProtectionState {
  Idle
  Burst
}

pub type Notification {
  Transition(from: ProtectionState, to: ProtectionState)
  Flushed(from: ProtectionState, to: ProtectionState, num_flushed: Int)
}

pub type ProtegeMessage(a) {
  Notification(notification: Notification)
  ProtegeMessage(message: a)
}

pub type ProtectedSubject(a) {
  ProtectedSubject(
    subject: Subject(a),
    sync_limit: Int,
    drop_limit: Int,
    sync_timeout: Int,
  )
}

type ProtectorState(a) {
  ProtectorState(
    next_time_window: DateTime,
    message_count: Int,
    protection_state: ProtectionState,
    protege_state: a,
    protector_stats: ProtectorStats,
  )
}

pub type ProtectorStats {
  ProtectorStats(
    num_async: Int,
    num_sync: Int,
    num_burst_dropped: Int,
    num_flushed: Int,
  )
}

pub type ProtectorMessage(a) {
  Async(message: a)
  Sync(message: a, reply_to: Subject(ProtectionState))
  Statistics(reply_to: Subject(ProtectorStats))
}

pub type SendStatus(a) {
  SentAsync
  SentSync(a)
  Dropped(Option(CallError(a)))
}

pub fn start(
  overload_protection: OverloadProtection,
  init_state: a,
  loop: fn(ProtegeMessage(b), a) -> Next(a),
) -> Result(ProtectedSubject(ProtectorMessage(b)), StartError) {
  otp_start(
    ProtectorState(
      add(now(), overload_protection.burst_window),
      0,
      Idle,
      init_state,
      ProtectorStats(0, 0, 0, 0),
    ),
    fn(msg: ProtectorMessage(b), state: ProtectorState(a)) {
      case msg {
        Statistics(reply_to) -> {
          process_send(reply_to, state.protector_stats)
          Continue(state)
        }
        _ -> {
          // Burst protection
          let current_time = now()
          let assert #(next_time_window, message_count) = case
            compare(current_time, state.next_time_window)
          {
            Gt -> #(add(current_time, overload_protection.burst_window), 1)
            _ -> #(state.next_time_window, state.message_count + 1)
          }
          let new_protection_state = case
            message_count > overload_protection.burst_limit
          {
            True -> Burst
            False -> Idle
          }
          // Check if we should send notification about updated state
          case new_protection_state == state.protection_state {
            False -> {
              loop(
                Notification(Transition(
                  state.protection_state,
                  new_protection_state,
                )),
                state.protege_state,
              )
              Nil
            }
            True -> Nil
          }
          // Check if messages needs to be flushed and notify if needed
          // Also increment stats for relevant categories
          let num_messages = queue_len(self())
          let #(next, new_protector_stats) = case
            num_messages > overload_protection.flush_limit
          {
            True -> {
              flush_messages()
              let next =
                loop(
                  Notification(Flushed(
                    state.protection_state,
                    state.protection_state,
                    num_messages,
                  )),
                  state.protege_state,
                )
              #(
                next,
                ProtectorStats(
                  ..state.protector_stats,
                  num_flushed: state.protector_stats.num_flushed + num_messages,
                ),
              )
            }
            False ->
              case new_protection_state {
                Idle ->
                  case msg {
                    Async(message: message) -> {
                      let next =
                        loop(ProtegeMessage(message), state.protege_state)
                      #(
                        next,
                        ProtectorStats(
                          ..state.protector_stats,
                          num_async: state.protector_stats.num_async + 1,
                        ),
                      )
                    }
                    Sync(message: message, ..) -> {
                      let next =
                        loop(ProtegeMessage(message), state.protege_state)
                      // Don't count the sync message until response has been sent
                      #(next, state.protector_stats)
                    }
                    _ -> #(Continue(state.protege_state), state.protector_stats)
                  }
                Burst -> #(
                  Continue(state.protege_state),
                  ProtectorStats(
                    ..state.protector_stats,
                    num_burst_dropped: state.protector_stats.num_burst_dropped + 1,
                  ),
                )
              }
          }
          // Add stats for sync message
          let protector_stats_with_sync = case msg {
            Sync(reply_to: subject, ..) -> {
              process_send(subject, state.protection_state)
              ProtectorStats(
                ..state.protector_stats,
                num_sync: state.protector_stats.num_sync + 1,
              )
            }
            Async(_) -> new_protector_stats
            _ -> new_protector_stats
          }
          case next {
            Continue(next_state) ->
              Continue(ProtectorState(
                protege_state: next_state,
                message_count: message_count,
                next_time_window: next_time_window,
                protection_state: new_protection_state,
                protector_stats: protector_stats_with_sync,
              ))
            Stop(reason) -> Stop(reason)
          }
        }
      }
    },
  )
  |> result.map(fn(sub) {
    ProtectedSubject(
      sub,
      overload_protection.sync_limit,
      overload_protection.drop_limit,
      overload_protection.sync_limit,
    )
  })
}

pub fn send(
  protected_subject: ProtectedSubject(ProtectorMessage(a)),
  message: a,
) -> SendStatus(ProtectionState) {
  let num_messages = queue_len(subject_owner(protected_subject.subject))
  case num_messages > protected_subject.drop_limit {
    True -> Dropped(None)
    _ ->
      case num_messages > protected_subject.sync_limit {
        True ->
          case
            process_try_call(
              protected_subject.subject,
              fn(subject) { Sync(message, subject) },
              protected_subject.sync_timeout,
            )
          {
            Ok(rsp) -> SentSync(rsp)
            Error(err) -> Dropped(Some(err))
          }
        False -> {
          process_send(protected_subject.subject, Async(message))
          SentAsync
        }
      }
  }
}

pub fn try_call(
  protected_subject: ProtectedSubject(ProtectorMessage(a)),
  make_request,
  timeout,
) {
  let num_messages = queue_len(subject_owner(protected_subject.subject))
  case num_messages > protected_subject.drop_limit {
    True -> Dropped(None)
    False ->
      case process_try_call(protected_subject.subject, make_request, timeout) {
        Ok(rsp) -> SentSync(rsp)
        Error(err) -> Dropped(Some(err))
      }
  }
}

pub fn statistics(protected_subject: ProtectedSubject(ProtectorMessage(a))) {
  process_try_call(
    protected_subject.subject,
    fn(subject) { Statistics(subject) },
    1000,
  )
}

type MessageQueueLen {
  MessageQueueLen(Int)
  Undefined
}

fn queue_len(pid: Pid) {
  let info = process_info(pid, atom.create_from_string("message_queue_len"))
  case info {
    Undefined -> {
      info
      |> io.debug()
      0
    }
    MessageQueueLen(queue_len) -> queue_len
  }
}

@external(erlang, "erlang", "process_info")
fn process_info(pid pid: Pid, item item: Atom) -> MessageQueueLen
