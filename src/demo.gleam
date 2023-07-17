import gleam/io
import gleam/int
import gleam/string
import gleam/option.{None, Option, Some}
import gleam/list.{fold, map, range}
import gleam/erlang/process.{Subject, call, send as process_send}
import gleam/otp/actor.{Continue}
import gleam/otp/task.{async, await}
import protege.{
  Async, Dropped, Flushed, Notification, OverloadProtection, ProtegeMessage,
  SentAsync, SentSync, Transition, send, start, statistics,
}
import birl/duration.{MilliSecond, decompose, new}
import birl/time.{difference, now}

type Message {
  Message(message: String, reply_to: Option(Subject(Int)))
}

pub fn main() {
  io.println("Starting test")
  let assert Ok(sub) =
    start(
      OverloadProtection(
        sync_limit: 10,
        drop_limit: 200,
        flush_limit: 1000,
        burst_protection: True,
        burst_window: new([#(1000, MilliSecond)]),
        burst_limit: 500,
        sync_timeout: 1000,
      ),
      0,
      fn(message: ProtegeMessage(Message), state) {
        let new_state = case message {
          Notification(notification) -> {
            // notification
            // |> io.debug()
            case notification {
              Transition(..) -> Nil
              Flushed(num_flushed: num_flushed, ..) ->
                io.println("Num flushed: " <> int.to_string(num_flushed))
            }
            state
          }
          ProtegeMessage(message) ->
            case message.reply_to {
              Some(subject) -> {
                io.println("Got total request")
                process_send(subject, state)
                state
              }
              None -> state + 1
            }
        }
        Continue(new_state)
      },
    )

  let start_time = now()
  let tasks =
    range(0, 10_000)
    |> map(fn(num) {
      // io.println("Sending: " <> int.to_string(num))
      async(fn() {
        case send(sub, Message(int.to_string(num), None)) {
          Dropped(_) -> #(0, 0, 1)
          //io.println("Dropped")
          SentAsync -> #(1, 0, 0)
          SentSync(_) -> #(0, 1, 0)
        }
      })
    })

  let stats =
    tasks
    |> fold(
      #(0, 0, 0),
      fn(count, task) {
        let assert #(async, sync, dropped) = await(task, 100)
        let assert #(tot_async, tot_sync, tot_dropped) = count
        #(async + tot_async, sync + tot_sync, dropped + tot_dropped)
      },
    )
  let end_time = now()
  let assert #(total_async, total_sync, total_dropped) = stats
  io.println("Total async: " <> int.to_string(total_async))
  io.println("Total sync: " <> int.to_string(total_sync))
  io.println("Total dropped: " <> int.to_string(total_dropped))

  let total_received =
    call(
      sub.subject,
      fn(subject) { Async(Message("total", Some(subject))) },
      4000,
    )
  statistics(sub)
  |> io.debug()
  io.println("Total received: " <> int.to_string(total_received))
  io.println("Total: " <> int.to_string(total_dropped + total_received))
  io.println(
    "Total time: " <> string.inspect(decompose(difference(end_time, start_time))),
  )
  process.sleep(4000)
}
